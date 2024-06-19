import asyncio
import websockets
import json
import polars as pl


async def on_message(message):
    print("Received message:", message)


async def run_websocket(api_key, secret_key):
    uri = "wss://paper-api.alpaca.markets/stream"
    # uri = 'wss: // stream.data.alpaca.markets / v2 / iex'
    async with websockets.connect(uri) as websocket:
        # Authenticate
        auth_data = {
            "action": "auth",
            "key": api_key,
            "secret": secret_key
        }
        await websocket.send(json.dumps(auth_data))
        response = await websocket.recv()

        auth_response = json.loads(response)
        pretty_json = json.dumps(auth_response, indent=4)
        print("Auth response:", pretty_json)

        if auth_response.get('data', '').get('status', '') != 'authorized':
            print("Authentication failed:", pretty_json)
            return

        # Subscribe to trade updates


        listen_message = {
            "action": "listen",
            "data": {
                "streams": ["trade_updates"]  # Replace with your desired symbol(s)
            }
        }
        await websocket.send(json.dumps(listen_message))
        message = await websocket.recv()
        await on_message(message)


        # Receive messages
        while True:
            try:
                await websocket.send(json.dumps(listen_message))
                message = await websocket.recv()
                await on_message(message)
            except websockets.exceptions.ConnectionClosed as e:
                print("Connection closed:", e)
                break
            except Exception as e:
                print("Error receiving message:", e)


# Run the event loop
if __name__ == "__main__":
    # asyncio.get_event_loop().run_until_complete(run_websocket())
    # asyncio.get_event_loop().run_forever()
    l = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"]
    }

    df = pl.DataFrame(l)
    print(df)