from fastapi import FastAPI,Request
import httpx 
import sys

coordinatorPort = 8000

permanent_storage = dict()
temp_storage = dict()

myPort = 7070
leftPort = None
rightPort = None 
isHead = False
isTail = False


sendToUrl = "http://localhost:"+str(coordinatorPort)+"/addAtTail/"+str(myPort)
res = httpx.get(sendToUrl)
try:
    res_data = res.json()
    if(res_data["status"] == "ok"):
        if(res_data["leftPort"] != "None"):
            leftPort = res_data["leftPort"]
        if(res_data["rightPort"] != "None"):
            rightPort = res_data["rightPort"]
        isHead = res_data["isHead"]
        isTail = res_data["isTail"]
    else:
        sys.exit("coordinator msg status is NOT OK")
except:

    print(res)
    print(res.text)
    sys.exit("failed at successful response from coordinator")
    
app = FastAPI()


@app.get("/handleChangeConfigAtInsertion")
async def handleChangeConfigAtInsertion(right_port: int):
    global myPort,rightPort,leftPort,isTail
    rightPort = right_port
    isTail = False

    return {
        "status" : "ok",
        "myPort" : myPort,
        "leftPort" : str(leftPort),
        "rightPort" : str(rightPort)
    }
    
@app.get("/getCurrentStatusOfNode")
async def getCurrentStatusOfNode():
    data = {
        "myPort" : myPort,
        "leftPort" : str(leftPort),
        "rightPort" : str(rightPort),
        "isHead" : isHead,
        "isTail" : isTail
    }
    return data

@app.get("/heartBeatStatus")
def heartBeatSendResponse():
    return {
        "status" : "ok",
        "port" : myPort
    }

@app.get("/configDueToNeighFailure")
def handleConfigChangeDueToNeighFailure(left_port:str,right_port:str,is_head:str,is_tail:str):
    global leftPort,rightPort,isHead,isTail

    #sorry for this long syntax basically too lazy to ensure validation from pydantic is implemented

    if(left_port == "None"):
        leftPort = None
    elif(left_port == "d"):
        leftPort = leftPort
    else:
        leftPort = int(left_port)
    
    if(right_port == "None"):
        rightPort = None
    elif(right_port == "d"):
        rightPort = rightPort
    else:
        rightPort = int(right_port)

    if(is_head == "True"):
        isHead = True
    elif(is_head == "d"):
        isHead = isHead
    else:
        isHead = False

    if(is_tail == "True"):
        isTail = True
    elif(is_tail == "d"):
        isTail = isTail
    else:
        isTail = False

    return{
        "status" : "ok",
        "port" : myPort,
        "leftPort" : str(leftPort),
        "rightPort" : str(rightPort),
        "isHead" : isHead,
        "isTail" : isTail
    }
