import subprocess
import os
import sys

def run_cargo_for_files(directory):
    if not os.path.isdir(directory):
        print(f"The directory '{directory}' does not exist.")
        sys.exit(1)

    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        
        if os.path.isfile(file_path):
            command = ["cargo", "run", "--", file_path]
            print(f"Running command: {' '.join(command)}")

            try:
                result = subprocess.run(command, text=True, check=True)
            except subprocess.CalledProcessError as e:
                print(f"An error occurred while processing {file_path}:")
                print(e)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_cargo.py <directory>")
        sys.exit(1)

    directory = sys.argv[1]
    run_cargo_for_files(directory)
