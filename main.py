from collections import Counter
import pandas as pd
import socket
import time

import ray

#ray.init()
ray.init(address='auto')
#ray.init(address='ray://35.88.4.105:10001')
print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
'''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))

@ray.remote
def f():
    time.sleep(0.001)
    # Return IP address.
    return socket.gethostbyname(socket.gethostname())

object_ids = [f.remote() for _ in range(10000)]
ip_addresses = ray.get(object_ids)

print('Tasks executed')
for ip_address, num_tasks in Counter(ip_addresses).items():
    print('    {} tasks on {}'.format(num_tasks, ip_address))
d = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data=d)
print(df)
#df.to_csv("/home/ubuntu/efs/out.csv", index=False)

# check to see if the data persists after destroying the cluster/EC2 instance and redeploying. 
# should work since we are mounting a file system external to the EC2 instance.
df2 = pd.read_csv("/home/ubuntu/efs/hello.txt")
print(df2)



# specify number of workers in config.yaml
@ray.remote
def do_some_work(x):
    time.sleep(1) # Replace this with work you need to do.
    return x

start = time.time()

results = ray.get([do_some_work.remote(x) for x in range(4)]) # ray.get() is a blocking operation. write your program such that ray.get() is called as late as possible.
print("duration =", time.time() - start)
print("results = ", results)