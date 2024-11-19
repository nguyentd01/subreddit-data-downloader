import json
import csv
import os
import requests
import zstandard as zstd
from collections import defaultdict
import tqdm
import configparser

def download_file(url, output_path):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_path, 'wb') as f:
            total_size = int(response.headers.get('content-length', 0))
            with tqdm.tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(output_path)) as pbar:
                for data in response.iter_content(chunk_size=1024):
                    f.write(data)
                    pbar.update(len(data))
        print(f"Downloaded: {output_path}")
    else:
        print(f"Failed to download {url}: {response.status_code}")

def decompress_file(zst_filepath, output_dir):
    with open(zst_filepath, 'rb') as compressed:
        dctx = zstd.ZstdDecompressor()
        output_filepath = os.path.join(output_dir, os.path.basename(zst_filepath).replace('.zst', ''))
        with open(output_filepath, 'wb') as decompressed:
            dctx.copy_stream(compressed, decompressed)
    print(f"Decompressing: {zst_filepath}")

def process_comments_in_chunks(comments_filepath, batch_size=1000):
    comments_by_post = defaultdict(list)
    try:
        with open(comments_filepath, 'r', encoding='utf-8') as f:
            for line in tqdm.tqdm(f, desc="Processing comments", unit="line"):
                try:
                    comment = json.loads(line)
                    link_id = comment.get("link_id")
                    parent_id = comment.get("parent_id")
                    if link_id and parent_id and parent_id.startswith("t3_"):
                        body = comment.get("body")
                        if ("[deleted]" not in body) and ("[removed]" not in body):
                            comments_by_post[link_id].append({
                                "body": comment.get("body"),
                                "score": comment.get("score", 0)
                            })
                        if len(comments_by_post[link_id]) >= batch_size:
                            yield link_id, comments_by_post[link_id]
                            comments_by_post[link_id] = []
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in comments file: {e}")
        for link_id, comments in comments_by_post.items():
            if comments:
                yield link_id, comments
    except FileNotFoundError:
        print(f"File not found: {comments_filepath}")

def match_posts_with_top_comments(posts_filepath, comments_generator, subreddit_name, writer, min_score):
    comments_by_post = defaultdict(list)
    for link_id, comments in tqdm.tqdm(comments_generator, desc="Loading comments", unit="post"):
        comments_by_post[link_id].extend(comments)

    try:
        with open(posts_filepath, 'r', encoding='utf-8') as f:
            for line in tqdm.tqdm(f, desc="Processing posts", unit="line"):
                try:
                    post = json.loads(line)
                    post_id = post.get("name")
                    title = post.get("title", "")
                    content = post.get("selftext", "")
                    if post.get("score", 0) >= min_score and (post_id in comments_by_post) and ("[deleted]" not in title) and ("[removed]" not in title) and ("[deleted]" not in content) and ("[removed]" not in content):
                        top_comment = max(comments_by_post[post_id], key=lambda c: c['score'], default=None)
                        if top_comment and top_comment["score"] >= 1:
                            writer.writerow({
                                "title": title,
                                "content": content,
                                "post_score": post.get("score"),
                                "top_comment": top_comment["body"],
                                "comment_score": top_comment["score"],
                                "subreddit": subreddit_name
                            })
                except json.JSONDecodeError as e:
                    print(f"Error decoding posts file: {e}")
    except FileNotFoundError:
        print(f"File not found: {posts_filepath}")

def save_config(output_directory):
    appdata_dir = os.path.join(os.getenv('APPDATA'), 'YourAppName', 'data')
    os.makedirs(appdata_dir, exist_ok=True)
    config_path = os.path.join(appdata_dir, 'config.ini')
    
    config = configparser.ConfigParser()
    config['Settings'] = {'output_directory': output_directory}
    with open(config_path, 'w') as configfile:
        config.write(configfile)
    print(f"Configuration saved: {config_path}")

def load_config():
    appdata_dir = os.path.join(os.getenv('APPDATA'), 'YourAppName', 'data')
    config_path = os.path.join(appdata_dir, 'config.ini')
    
    config = configparser.ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)
        return config['Settings'].get('output_directory', '')
    return ''

def process_files_in_directory(directory_path, output_directory, min_score, writer):
    for filename in os.listdir(directory_path):
        if filename.endswith("_comments"):
            comments_filepath = os.path.join(directory_path, filename)
            comments_generator = process_comments_in_chunks(comments_filepath)
        elif filename.endswith("_submissions"):
            posts_filepath = os.path.join(directory_path, filename)
            subreddit_name = filename.split('_')[0]
            match_posts_with_top_comments(posts_filepath, comments_generator, subreddit_name, writer, min_score)

    for filename in os.listdir(directory_path):
        if not filename.endswith(".csv"):
            os.remove(os.path.join(directory_path, filename))
            print(f"Deleted unnecessary file: {filename}")

def main():
    output_directory = load_config() or input("Enter the output directory (e.g., D:\\Output): ")
    min_score = int(input("Enter the minimum score for posts to be included: "))

    os.makedirs(output_directory, exist_ok=True)
    save_config(output_directory)

    output_csv_path = os.path.join(output_directory, "merged_data.csv")
    fieldnames = ['title', 'content', 'post_score', 'top_comment', 'comment_score', 'subreddit']
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        subreddits = []
        while True:
            subreddit_name = input("Enter the subreddit name (e.g., AskReddit) or 'done' to finish: ")
            if subreddit_name.lower() == 'done':
                break
            subreddits.append(subreddit_name)

        for subreddit_name in subreddits:
            submissions_url = f"https://the-eye.eu/redarcs/files/{subreddit_name}_submissions.zst"
            comments_url = f"https://the-eye.eu/redarcs/files/{subreddit_name}_comments.zst"

            download_file(submissions_url, os.path.join(output_directory, f"{subreddit_name}_submissions.zst"))
            download_file(comments_url, os.path.join(output_directory, f"{subreddit_name}_comments.zst"))

            decompress_file(os.path.join(output_directory, f"{subreddit_name}_submissions.zst"), output_directory)
            decompress_file(os.path.join(output_directory, f"{subreddit_name}_comments.zst"), output_directory)

            process_files_in_directory(output_directory, output_directory, min_score, writer)

    print(f"Merged data saved to: {output_csv_path}")

if __name__ == "__main__":
    main()
