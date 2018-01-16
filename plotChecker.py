#!/usr/bin/python3

SCOOP_SIZE = 64
NUM_SCOOPS = 4096
NONCE_SIZE = NUM_SCOOPS * SCOOP_SIZE


if __name__ == "__main__":
    import os
    import sys
    try:
        from colorama import init, Fore, Style
        BRIGHTRED = Style.BRIGHT + Fore.RED
        BRIGHTGREEN = Style.BRIGHT + Fore.GREEN
        BRIGHTBLUE = Style.BRIGHT + Fore.BLUE
        BRIGHTYELLOW = Style.BRIGHT + Fore.YELLOW
        RESET_ALL = Style.RESET_ALL
        init()
    except:
        BRIGHTRED = BRIGHTGREEN = BRIGHTBLUE = BRIGHTYELLOW = RESET_ALL = ""
    if len(sys.argv) < 2:
        print(BRIGHTGREEN + "BURST plot checker (version 1.0)\nChecking and validation of plots for BURST")
        print("Translated into Python from Blago's C++ version which is only for Windows.")
        print(BRIGHTBLUE + "Usage: %s PATH1 [PATH2] ..." % sys.argv[0])
        print(BRIGHTGREEN + "Example: %s /media/data/plots" % sys.argv[0] + RESET_ALL)
        sys.exit(1)
    for plotsDirName in sys.argv[1:]:
        if os.path.isdir(plotsDirName):
            pathNames = [ os.path.join(plotsDirName, fileName) for fileName in os.listdir(plotsDirName) ]
        elif os.path.exists(plotsDirName):
            pathNames = [ plotsDirName ]
            plotsDirName = os.path.dirname(plotsDirName)
        else:
            print(BRIGHTRED + f"Error: File {plotsDirName} not found!" + RESET_ALL)
            continue
        for pathName in pathNames:
            # Ignore directories and invalid file names
            fileName = os.path.basename(pathName)
            if os.path.isdir(pathName) or (fileName.count("_") != 3) or "." in fileName:
                continue
            key, nonce, nonces, stagger = [ int(x) for x in fileName.split("_") ]
            fileSize = os.path.getsize(pathName)
            if fileSize == nonces * NONCE_SIZE:
                print(BRIGHTGREEN + f"OK {pathName}" + RESET_ALL)
                continue
            # Why is an optimized plot invalid (in Blago's plot checker)?
            #if nonces == stagger:
            #    print(BRIGHTRED + f"INVALID (replot) {pathName}" + RESET_ALL)
            #    continue
            newNonces = int(fileSize / NONCE_SIZE / stagger) * stagger
            newPathName = os.path.join(plotsDirName, f"{key}_{nonce}_{newNonces}_{stagger}")
            try:
                os.rename(pathName, newPathName)
                print(BRIGHTYELLOW + f"File {pathName} renamed to {newPathName}" + RESET_ALL)
            except Exception as exc:
                print(BRIGHTRED + f"FAILED renaming file: {pathName}\n{exc}" + RESET_ALL)
                continue
            newFileSize = newNonces * NONCE_SIZE
            if fileSize != newFileSize:
                try:
                    os.truncate(newPathName, newFileSize)
                    print(BRIGHTBLUE + f"TRUNCATED {newPathName} to {newFileSize} bytes" + RESET_ALL)
                except:
                    print(BRIGHTRED + f"FAILED truncating file: {newPathName}\n{exc}" + RESET_ALL)
