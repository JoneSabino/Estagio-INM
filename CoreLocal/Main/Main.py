from Parser.FileReader import *
import time

while 1:
    if verifyShare() == True:
        do()
    else:
        time.sleep(300)




