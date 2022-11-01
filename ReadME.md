#run uvicorn:app --port {portNo mentioned in file} --reload


#completed
coordinator - nodes add/delete ops,write data 
nodes - communication b/w neighbour nodes, write data and commits
reading data api in node
dirty reads handled 
adding new tail at node adds the prev committed data from prev tail

###TO_DO

what if tail node collapses just b4 committing new values 

and testing 
