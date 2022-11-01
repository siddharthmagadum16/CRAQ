from asyncio import proactor_events
from multiprocessing.reduction import send_handle
from sqlite3 import paramstyle

from fastapi import FastAPI
import httpx

from fastapi_utils.tasks import repeat_every

ports = []

headPort = None
tailPort = None
ports = []



app = FastAPI()



def changeConfigAtInsertion(new_node_port):
    tempLeftPort = ports[-1]
    payLoadData = {"right_port" : new_node_port}
    sendToLeftPort = "http://localhost:"+str(tempLeftPort)+"/handleChangeConfigAtInsertion"
    res = httpx.get(sendToLeftPort,params=payLoadData)
    print(res.json())

def changeConfigDueToNodeCrash(index):
    print("inside changeConfigDueToNodeCrash")
    global headPort,tailPort,ports
    print(index,"<===index No")
    if(index == 0):
        print("inside index = 0")
        if(len(ports)==1):
            print("inside len(index) = 1")
            print("all Nodes Failed No writes can be executed")
            print("removed port number ===> ",ports[index])
            ports.pop(index)
            headPort = None
            tailPort = None
        else:
            print("inside index=1 else's")
            temp_port = ports[index+1]
            if(index+2 <= len(ports)-1):
                right_port = ports[index+2]
                temp_is_tail = False
            else:
                right_port = None
                temp_is_tail = True
            payLoadData = {
                "left_port": str(None),
                "right_port" : str(right_port),
                "is_head" : str(True),
                "is_tail" : str(temp_is_tail)
            }
            sendToAddress = "http://localhost:"+str(temp_port)+"/configDueToNeighFailure"
            res = httpx.get(sendToAddress,params=payLoadData)
            ports.pop(index)
            headPort = ports[0]
            print("|||||||||||||||||||||||||||||||||")
            print(res.json())
            print("remaining ports : " ,ports)


    elif(index == len(ports)-1):
        print("print inside elif")
        sendToAddress = "http://localhost:"+str(ports[index-1])+"/configDueToNeighFailure"
        payLoadData = {
            "left_port" : "d",
            "right_port" : "None",
            "is_head" : "d",
            "is_tail" : str(True)
        }
        res = httpx.get(sendToAddress,params=payLoadData)
        ports.pop(index)
        tailPort = ports[-1]

    else:
        print("inside else")
        sendToAddress = "http://localhost:"
        leftPayLoadData = {
            "left_port" : "d",
            "right_port" : ports[index+1],
            "is_head" : "d",
            "is_tail" : "d"
        }
        resLeft = httpx.get(sendToAddress+str(ports[index-1])+"/configDueToNeighFailure",params=leftPayLoadData)
        rightPayLoadData = {
            "left_port" : ports[index-1],
            "right_port" : "d",
            "is_head" : "d",
            "is_tail" : "d"
        }
        resRight = httpx.get(sendToAddress+str(ports[index+1])+"/configDueToNeighFailure",params=rightPayLoadData)
        ports.pop(index)
        print("|||||||||||||||||||||||||||")
        print(resLeft.json())
        print(resRight.json())



@app.on_event("startup")
@repeat_every(seconds = 8)
def heartBeatCheck():
    global ports
    for i in range(0,len(ports)):
        heartbeatUrl = "http://localhost:"+str(ports[i])+"/heartBeatStatus"
        try:
            res = httpx.get(heartbeatUrl)
            print(res.json())
        except:
            print(ports[i]," is dead")
            changeConfigDueToNodeCrash(i)


@app.get("/addAtTail/{new_node_port}")
def addNewNodeAtTail(new_node_port : int):
    global headPort,tailPort
    if(headPort == None):
        headPort = new_node_port
        tailPort = new_node_port
        ports.append(new_node_port)
        return{
            "status" : "ok",
            "leftPort" : "None",
            "rightPort" : "None",
            "isHead" : True,
            "isTail" : True
        }
    else :
        temp_left_port = ports[-1]
        changeConfigAtInsertion(new_node_port)
        tailPort = new_node_port
        ports.append(new_node_port)
        return{
            "status" : "ok",
            "leftPort" : temp_left_port,
            "rightPort" : "None",
            "isHead" : False,
            "isTail" : True
        }

@app.get("/getCurrentStatusOfNode")
async def getCurrentStatusOfNode():
    data = {
        "headPort" : str(headPort),
        "tailPort" : str(tailPort),
        "ports" : ports
    }
    return data


def handleForwardWriteToNodeFromCoordinator(key,value):
    payLoadDataForVersionNumbers = {
        "key" : key
    }

    headVersion = httpx.get("http://localhost:"+str(headPort)+"/getCurrentVersionOfKey",params=payLoadDataForVersionNumbers).json()

    newVersionNumber = headVersion["version"] + 1
    payLoadData = {
        "key" : key,
        "value" : value,
        "version_no" : newVersionNumber
    }
    httpx.get("http://localhost:"+str(headPort)+"/writeKeyValueWithVersion",params=payLoadData)
    return newVersionNumber

@app.get("/writeData")
def handleWriteData(key:str , value:str):
    if(len(ports) == 0):
        return {
            "status" : "error",
            "msg" : "All Nodes Failed"
        }

    else:
        newVersionNumber=handleForwardWriteToNodeFromCoordinator(key,value)
        return {
            "status" : "in progress",
            "key" : key,
            "value" : value,
            "version_no" : newVersionNumber
        }

@app.get("/getTailPort")
def handleTailPortInfo():
    return {
        "tailPort" : tailPort
    }