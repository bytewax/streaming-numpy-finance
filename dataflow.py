'''
Input: ticker list
For example: ['AMZN', 'MSFT']

Output: data stream (tuple) with 1-minute time window containing ticker, metadata,
time, min, max, first price, last price and volume for each window
For example:
...
ADD EXAMPLE HERE
...
'''
import base64
import json
from datetime import datetime, timedelta, timezone

import numpy as np

from bytewax import operators as op
import bytewax.operators.window as win

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async
from bytewax.operators.window import EventClockConfig, TumblingWindow

import websockets
from ticker_pb2 import Ticker

## input
ticker_list = ['AMZN', 'MSFT']
# we can also use BTC-USD outside of stock exchange opening hours
#ticker_list = ['BTC-USD']

# Function deserializing Protobuf messages
def deserialize(message):
    '''Use the imported Ticker class to deserialize 
    the protobuf message

    returns: ticker id and ticker object
    '''
    ticker_ = Ticker()
    message_bytes = base64.b64decode(message)
    ticker_.ParseFromString(message_bytes)
    return ticker_.id, ticker_

# Function yielding deserialized data from YahooFinance
async def _ws_agen(worker_tickers):
    url = "wss://streamer.finance.yahoo.com/"
    # Establish connection to Yahoo Finance with WebSockets
    async with websockets.connect(url) as websocket:
        # Subscribe to tickers
        msg = json.dumps({"subscribe": worker_tickers})
        await websocket.send(msg)
        await websocket.recv()

        while True:
            # Receive updates
            msg = await websocket.recv()
            # Deserialize
            msg_ok = deserialize(msg)
            yield msg_ok

# Yahoo partition class inherited from Bytewax input StatefulSourcePartition class
class YahooPartition(StatefulSourcePartition):
    '''
    Input partition that maintains state of its position.
    '''
    def __init__(self, worker_tickers):
        '''
        Get deserialized messages from Yahoo Finance and batch them
        up to 0,5 seconds or 100 messages.
        '''
        agen = _ws_agen(worker_tickers)
        self._batcher = batch_async(agen, timedelta(seconds=0.5), 100)

    def next_batch(self):
        '''
        Attempt to get the next batch of items.
        '''
        return next(self._batcher)

    def snapshot(self):
        '''
        Snapshot the position of the next read of this partition.
        Returned via the resume_state parameter of the input builder.
        '''
        return None

# Yahoo source class inherited from Bytewax input FixedPartitionedSource class
class YahooSource(FixedPartitionedSource):
    '''
    Input source with a fixed number of independent partitions.
    '''
    def __init__(self, worker_tickers):
        '''
        Initialize the class with the ticker list
        '''
        self.worker_tickers = worker_tickers

    def list_parts(self):
        '''
        List all partitions the worker has access to.
        '''
        return ["single-part"]

    def build_part(self, step_id, for_key, _resume_state):
        '''
        Build anew or resume an input partition.
        Returns the built partition
        '''
        return YahooPartition(self.worker_tickers)


# Creating dataflow and input
# ADD COMMENT WITH EXPECTED VALUE HERE
flow = Dataflow("yahoofinance")
inp = op.input(
    "input", flow, YahooSource(ticker_list)
)

def build_array():
    '''
    Build an empty array
    '''
    return np.empty((0,3))

def acc_values(np_array, ticker):
    '''
    Accumulator function; inserts time, price and volume values into the array
    '''
    return np.insert(np_array, 0, np.array((ticker.time, ticker.price, ticker.dayVolume)), 0)

def get_event_time(ticker):
    '''
    Retrieve event's datetime from the input (Must be UTC)
    '''
    return datetime.utcfromtimestamp(ticker.time/1000).replace(tzinfo=timezone.utc)

# Configure the `fold_window` operator to use the event time
clock_config = EventClockConfig(get_event_time, wait_for_system_duration=timedelta(seconds=10))

# Add a 5 seconds tumbling window, that starts at the beginning of the minute
align_to = datetime.now(timezone.utc)
align_to = align_to - timedelta(
    seconds=align_to.second, microseconds=align_to.microsecond
)
window_config = TumblingWindow(length=timedelta(seconds=60), align_to=align_to)
# ADD COMMENT WITH EXPECTED VALUE HERE
window = win.fold_window("1_min", inp, clock_config, window_config, build_array, acc_values)

def calculate_features(ticker__data):
    '''
    Data analysis function; 
    Returns metadata, time, min, max, first price, last price and volume for each window
    '''
    ticker, data = ticker__data
    win_data = data[1]
    return (
        ticker,
        data[0], # metadata
        {
            "time":win_data[-1][0],
            "min":np.amin(win_data[:,1]), 
            "max":np.amax(win_data[:,1]),
            "first_price":win_data[:,1][-1], 
            "last_price":win_data[:,1][0],
            "volume":win_data[:,2][0] - win_data[:,2][-1]
        }
    )

features = op.map("features", window, calculate_features)

# Output
# ADD COMMENT WITH EXPECTED VALUE HERE
op.output("out", features, StdOutSink())
