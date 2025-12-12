import subprocess

with open("commands.txt", "r") as f:
    commands = f.readlines()

for cmd in commands:
    cmd = cmd.strip()
    if cmd:
        print(f"Running: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True) 
        if cmd.startswith("tail" \
        ""):
            print(result.stdout)   