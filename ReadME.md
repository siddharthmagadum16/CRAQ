# Commands
    run uvicorn:app --port {portNo mentioned in file} --reload
    ```

    uvicorn coordinator.coordinator:app --port 8000 --reload
    uvicorn node1.node:app --port 6969 --reload
    uvicorn node2.node:app --port 7070 --reload
    uvicorn node3.node:app --port 9090 --reload
    ```

# Completed
coordinator - nodes add/delete ops,write data
nodes - communication b/w neighbour nodes, write data and commits
reading data api in node
dirty reads handled

### TO_DO

adding new tail at node should get data from prev tail node
commit message to the client if requested after committing the values
and testing

