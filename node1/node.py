
from fastapi import FastAPI,BackgroundTasks
import httpx
import sys
import time

coordinatorPort = 8000

permanent_storage = {
    "a" : {
        "dirty" : False,
        "value" : {
            1: "one"
        }
    }
}

myPort = 6969
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
            res_from_prev_tail = httpx.get("http://localhost:"+str(leftPort)+"/sendCommittedDataForNewTailNode").json()
            permanent_storage = res_from_prev_tail["dataDict"]
            print(permanent_storage," has been added into tail")
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


@app.get("/sendCommittedDataForNewTailNode")
def handleSendCommittedDataForNewTailNode():
    return {
        "status" : "ok",
        "dataDict" : permanent_storage
    }


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

    # delete all but latest committed versions

    while len(permanent_storage[key]["value"]) > 0:
        oldest_key = permanent_storage[key]["value"].keys()[0]
        if(oldest_key < version_no):
            permanent_storage[key]['value'].pop(oldest_key)
        else: break
    permanent_storage[key]['dirty'] = False #as it is guranteed that the first key version is committed

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

@app.get("/commitedTailNodeData")
def handleCommitedTailNodeData(key:str):
    if(key in permanent_storage):
        temp_version = sorted(permanent_storage[key]["value"])[-1]
        return {
            "status" : "found",
            "key" : key,
            "version" : temp_version,
            "value" : permanent_storage[key]["value"][temp_version]
        }
    else:
        return {
            "status" : "error",
            "key" : key,
            "version" : 0,
            "value" : " "
        }
        

@app.get("/getData")
def handleGetData(key : str):
    global permanent_storage
    if(key in permanent_storage):

        if(permanent_storage[key]["dirty"] == True):

            payLoadData = {
                "key" : key
            }

            res = httpx.get("http://localhost:"+str(coordinatorPort)+"/getTailPort")
            try:

                res = res.json()
                tail_node_port = res["tailPort"]
                temp_committedData = httpx.get("http://localhost:"+str(tail_node_port)+"/commitedTailNodeData",params=payLoadData).json()

                if(temp_committedData["status"] == "found"):

                    if(temp_committedData["version"] >= sorted(permanent_storage[key]["value"])[-1]):

                        permanent_storage[key]["dirty"] = False
                        permanent_storage[key]["value"] = {
                            temp_committedData["version"] : temp_committedData["value"]
                        }

                        return {
                            "status" : "ok from tail node okokok",
                            "key" : key,
                            "value" : temp_committedData["value"]
                        }

                    else:

                        return{
                            "status" : "ok from tail node",
                            "key" : key,
                            "value" : temp_committedData["value"]
                        }
                
                else:

                    return{
                        "status" : "error",
                        "msg" : "key not present"
                     }

            except:

                return {
                    "status" : "error",
                    "msg" : "pls try again"
                }
            
        else:

            return {
                "status" : "ok from my node",
                "key" : key,
                "value" : permanent_storage[key]["value"][sorted(permanent_storage[key]["value"])[0]]
            }

    else:
        
        return {
            "status" : "error",
            "msg" : "key not present"
        }

