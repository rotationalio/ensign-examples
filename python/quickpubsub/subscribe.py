import asyncio

from pyensign.ensign import authenticate, subscriber

@authenticate()
async def main():
    await print_events()

@subscriber("counter")
async def print_events(events):
    async for event in events:
        print(event)

if __name__ == "__main__":
    asyncio.run(main())