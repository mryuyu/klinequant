"""Debug ZMQ DEALER-REP communication"""
import sys
import asyncio

sys.path.insert(0, ".")
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import zmq
import zmq.asyncio
from protocol.codec import serialize_message, deserialize_message
from protocol.messages import Message


async def main():
    port = 15799
    ctx = zmq.asyncio.Context()

    # REP server
    rep = ctx.socket(zmq.REP)
    rep.bind(f"tcp://127.0.0.1:{port}")

    # DEALER client
    dealer = ctx.socket(zmq.DEALER)
    dealer.connect(f"tcp://127.0.0.1:{port}")

    # DEALER sends [b"", data]
    msg = Message(msg_type="TEST", source="debug", payload={"hello": "world"})
    payload = serialize_message(msg)
    print(f"DEALER sending: {len(payload)} bytes")
    await dealer.send_multipart([b"", payload])

    # REP receives
    raw = await rep.recv()
    print(f"REP recv (single): {len(raw)} bytes, data[:20]={raw[:20]}")

    # Check if we can deserialize
    try:
        req = deserialize_message(raw)
        print(f"REP deserialized: {req.msg_type} / {req.payload}")
    except Exception as e:
        print(f"REP deserialize FAILED: {e}")
        # Try multipart
        print("Trying recv_multipart...")
        # REP is now in "sent reply" state, can't recv again
        # So let's restart

    # REP sends reply
    resp = Message(msg_type="OK", source="server", payload={"result": 42})
    resp_data = serialize_message(resp)
    print(f"REP sending: {len(resp_data)} bytes")
    await rep.send(resp_data)

    # DEALER receives
    parts = await dealer.recv_multipart()
    print(f"DEALER recv_multipart: {len(parts)} frames")
    for i, p in enumerate(parts):
        print(f"  frame[{i}]: {len(p)} bytes, data[:20]={p[:20]}")

    # Try deserialize last frame
    try:
        result = deserialize_message(parts[-1])
        print(f"DEALER deserialized: {result.msg_type} / {result.payload}")
    except Exception as e:
        print(f"DEALER deserialize FAILED: {e}")

    # Also try single recv
    dealer.close()
    rep.close()

    # Test 2: single recv on DEALER
    rep2 = ctx.socket(zmq.REP)
    rep2.bind(f"tcp://127.0.0.1:{port}")
    dealer2 = ctx.socket(zmq.DEALER)
    dealer2.connect(f"tcp://127.0.0.1:{port}")

    await dealer2.send_multipart([b"", payload])
    raw2 = await rep2.recv()
    req2 = deserialize_message(raw2)
    print(f"\nTest2 REP recv OK: {req2.msg_type}")

    resp2_data = serialize_message(resp)
    await rep2.send(resp2_data)

    # DEALER: single recv
    single = await dealer2.recv()
    print(f"DEALER single recv: {len(single)} bytes, data[:20]={single[:20]}")
    try:
        result2 = deserialize_message(single)
        print(f"DEALER single deserialize OK: {result2.msg_type} / {result2.payload}")
    except Exception as e:
        print(f"DEALER single deserialize FAILED: {e}")

    dealer2.close()
    rep2.close()
    ctx.term()
    print("\nDone!")


asyncio.run(main())
