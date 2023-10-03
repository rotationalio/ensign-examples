import asyncio

from pyensign.ensign import authenticate, publisher

@authenticate()
async def main():
    for i in range(10):
        await count(i)

    # Wait for the events to be published
    await asyncio.sleep(1)

@publisher("counter")
async def count(i):
    return {"count": i}

if __name__ == "__main__":
    asyncio.run(main())