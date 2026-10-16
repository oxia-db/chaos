# Reads log lines on stdin and prints "<source>\t<signature>\t<line>" for every warning or error.
# A signature is the level and message with numbers and IDs replaced, so repeats group together.
import re
import sys

source = sys.argv[1]
ansi = re.compile(r"\x1b\[[0-9;]*m")
for line in sys.stdin:
    line = ansi.sub("", line.rstrip("\n"))
    oxia = re.search(r"\b(WRN|ERR|FTL|PNC)\b\s+(.*?)(\s+\w[\w-]*=|$)", line)
    java = re.search(r"\] (WARN|ERROR) (\S+) - (.*)", line)
    if oxia:
        signature = oxia.group(1) + " " + oxia.group(2)
    elif java:
        # Key=value pairs come before the message (RetryUtils) or after it (client retries). Keep
        # only status=, which tells retries apart, and drop everything from the first trailing pair.
        leading, message = re.match(r"((?:\w+=\S+ )*)(.*)", java.group(3)).groups()
        status = re.search(r"status=\S+ ", leading)
        message = (status.group(0) if status else "") + re.split(r" \w+=", message)[0]
        signature = java.group(1) + " " + java.group(2).split(".")[-1] + " - " + message
    elif re.search(r"panic|fatal|Exception in thread|OutOfMemory", line, re.I):
        signature = "CRASH " + line[:120]
    else:
        continue
    signature = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "<id>", signature)
    signature = re.sub(r"\d+(\.\d+)*", "N", signature)
    print(f"{source}\t{signature[:160]}\t{line[:600]}")
