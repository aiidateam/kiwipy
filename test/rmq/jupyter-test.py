# %%

import kiwipy

comm = kiwipy.connect('amqp://127.0.0.1')


def rpc_call(_comm, msg):
    print("Got your message: {}".format(msg))
    return "Thanks!"


comm.add_rpc_subscriber(rpc_call, 'tester')

response = comm.rpc_send('tester', 'hello!')

# %%

print(response.result())

# %%

comm.close()
