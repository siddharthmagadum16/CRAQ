## Commands
    run uvicorn:app --port {portNo mentioned in file} --reload
    ```

    uvicorn coordinator.coordinator:app --port 8000 --reload
    uvicorn node1.node:app --port 6969 --reload
    uvicorn node2.node:app --port 7070 --reload
    uvicorn node3.node:app --port 9090 --reload
    ```

### Completed
Coordinator - nodes add/delete ops,write data
Nodes - communication b/w neighbour nodes, write data and commits
Reading data api in node
Dirty reads handled
Handeled commit versions if the tail collapses



