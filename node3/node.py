from logging import INFO
from operator import is_
from pickle import FALSE
from turtle import left
from typing_extensions import Self
from fastapi import FastAPI,BackgroundTasks
import httpx 
import sys

coordinatorPort = 8000

permanent_storage = {
    "a" : {
        "dirty" : False,
        "value" : {
            1 : "one"
        }
    }
}

myPort = 9090
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

@app.get("/getCurrentVersionOfKey")
def handleGetCurrentVersionOfKey(key:str):
    print("inside handlegetcurrentversion ==== ")
    if(key in permanent_storage):
        print("inside if of handlegetcurrentversion")
        return {
            "version" : sorted(permanent_storage[key]["value"])[-1]
        }
    else:
        print("inside else of handlegetcurrentversion")
        return {
            "version" : 0
        }

def handleWriteDataToForwardNodes(key,value,version_no):
    global permanent_storage
    if(isTail):
        handleCommitDataToBackwardNodes(key,value,version_no)
    else :
        payLoadData = {
            "key" : key,
            "value" : value,
            "version_no" : version_no
        }
        res = httpx.get("http://localhost:"+str(rightPort)+"/writeKeyValueWithVersion",params=payLoadData)

def handleCommitDataToBackwardNodes(key,value,version_no):
    if(isHead == False):
        payLoadData = {
            "key" : key,
            "value" : value,
            "version_no" : version_no
        }
        res = httpx.get("http://localhost:"+str(leftPort)+"/commitData",params=payLoadData)

@app.get("/commitData")
def handleCommitRequest(key:str,value:str,version_no:int,background_tasks: BackgroundTasks):
    global permanent_storage
    curr_highest_version_no = sorted(permanent_storage[key]["value"])[-1]

    if(version_no >= curr_highest_version_no):
        permanent_storage[key]["dirty"] = False
        permanent_storage[key]["value"] = {
            version_no : value
        }

    background_tasks.add_task(handleCommitDataToBackwardNodes,key,value,version_no)
    return {
        "status " : "committed",
        "myPort"  : myPort,
        "key" : key,
        "version" : version_no
    }

@app.get("/checkDataDebug")
def checkDataDebug():
    return {
        "dict" : permanent_storage
    }


@app.get("/writeKeyValueWithVersion")
def handleWriteAtNodeWithVersionn(key:str,value:str,version_no:int,background_tasks:BackgroundTasks):
    global permanent_storage

    if(key in permanent_storage):
        
        if(isTail):
            permanent_storage[key]["dirty"] = False
            permanent_storage[key]["value"] = {
                version_no : value
            }
            background_tasks.add_task(handleCommitDataToBackwardNodes,key,value,version_no)
            return {
                "status" : "ok",
                "version" : version_no
            }
        else:
            permanent_storage[key]["dirty"] = True
            permanent_storage[key]["value"][version_no] = value
            background_tasks.add_task(handleWriteDataToForwardNodes,key,value,version_no)
            return {
                "status" : "ok",
                "version" : version_no
            }

    else:
        if(isTail):
            permanent_storage[key] = {
                "dirty" : False,
                "value" : {
                    version_no : value
                }
            }
            background_tasks.add_task(handleCommitDataToBackwardNodes,key,value,version_no)
            return {
                "status" : "ok",
                "version" : version_no
            }
        
        else:
            permanent_storage[key] = {
                "dirty" : True,
                "value" : {
                    version_no : value
                }
            }
            background_tasks.add_task(handleWriteDataToForwardNodes,key,value,version_no)
            return {
                "status" : "ok",
                "version" : version_no
            }
