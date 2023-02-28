import base64
import json
from datetime import datetime, timedelta, timezone

import numpy as np

from bytewax.dataflow import Dataflow
from bytewax.inputs import ManualInputConfig, distribute
from bytewax.window import EventClockConfig, TumblingWindowConfig
from bytewax.execution import run_main
from bytewax.outputs import StdOutputConfig


from websocket import create_connection
from ticker_pb2 import Ticker

## input
ticker_list = ['AMZN', 'MSFT']

def yf_input(worker_tickers, state):
        ws = create_connection("wss://streamer.finance.yahoo.com/")
        ws.send(json.dumps({"subscribe": worker_tickers}))
        while True:
            yield state, ws.recv()


def input_builder(worker_index, worker_count, resume_state):
    state = resume_state or None
    worker_tickers = list(distribute(ticker_list, worker_index, worker_count))
    print({"subscribing to": worker_tickers})
    return yf_input(worker_tickers, state)


flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))


# Protobuf deserialization
def deserialize(message):
    '''Use the imported Ticker class to deserialize 
    the protobuf message

    returns: ticker id and ticker object
    '''
    ticker_ = Ticker()
    message_bytes = base64.b64decode(message)
    ticker_.ParseFromString(message_bytes)
    return ticker_.id, ticker_
    

flow.map(deserialize)


def build_array():
    return np.empty((0,3))


# This is the accumulator function, and outputs a numpy array of time and price
def acc_values(np_array, ticker):
    return np.insert(np_array, 0, np.array((ticker.time, ticker.price, ticker.dayVolume)), 0)


# This function instructs the event clock on how to retrieve the
# event's datetime from the input.
# Note that the datetime MUST be UTC. If the datetime is using a different
# representation, we would have to convert it here.
def get_event_time(ticker):
    return datetime.utcfromtimestamp(ticker.time/1000).replace(tzinfo=timezone.utc)


# Configure the `fold_window` operator to use the event time.
cc = EventClockConfig(get_event_time, wait_for_system_duration=timedelta(seconds=10))

# And a 5 seconds tumbling window, that starts at the beginning of the minute
start_at = datetime.now(timezone.utc)
start_at = start_at - timedelta(
    seconds=start_at.second, microseconds=start_at.microsecond
)
wc = TumblingWindowConfig(start_at=start_at, length=timedelta(seconds=60))
flow.fold_window("1_min", cc, wc, build_array, acc_values)
flow.inspect(print)

def calculate_features(ticker__data):
    ticker, data = ticker__data
    return (ticker, {"time":data[-1][0], "min":np.amin(data[:,1]), "max":np.amax(data[:,1]), "first_price":data[:,1][-1], "last_price":data[:,1][0], "volume":data[:,2][0] - data[:,2][-1]})

flow.map(calculate_features)

flow.capture(StdOutputConfig())

if __name__ == "__main__":
    run_main(flow)