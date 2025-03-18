import argparse
import asyncio
import os
import shutil
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_folder(folder_path):
    file_paths = []
    try:
        for root, _, files in os.walk(folder_path):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, folder_path)
                file_paths.append(rel_path)
    except Exception as e:
        logger.error(f"Error reading folder {folder_path}. Nothing to copy.")
    return file_paths

async def copy_file(from_path, to_path):
    logger.info(f"Copying {from_path} to {to_path}")
    try:
        await asyncio.to_thread(shutil.copy, from_path, to_path)
    except Exception as e:
        logger.error(f"Error copying {from_path} to {to_path}")
    
def flatten_to_dir_type(file_path: str):
    path, ext = os.path.splitext(file_path)
    
    # build new name to not override existing files in output directory
    path_parts = path.split('/')
    for index, part in enumerate(path_parts[:-1]):
        path_parts[index] = part[0]
        
    new_name = ''.join(path_parts) + ext
    
    return (ext[1:], new_name)
    
def main():
    parser = argparse.ArgumentParser(description='Copy files from source directory to target directory')
    parser.add_argument('--id', type=str, help='Source directory')
    parser.add_argument('--od', type=str, help='Target directory')
    args = parser.parse_args()
    
    os.makedirs(args.od, exist_ok=True)
    
    files = read_folder(args.id)
    
    tasks = []
    for file in files:
        ext, new_name = flatten_to_dir_type(file)
        target_path = os.path.join(args.od, ext, new_name)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        tasks.append(copy_file(os.path.join(args.id, file), target_path))
        
    async def run_tasks():
        await asyncio.gather(*tasks)
        
    asyncio.run(run_tasks())

if __name__ == '__main__':
    main()
