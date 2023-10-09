# Building a real-time stock market chart using PyEnsign, Streamlit, and Plotly
Streamlit is a python library that enables data professionals to create web applications for data products without requiring front end experience.

This example demonstrates how to build a real-time stock market chart by creating a publisher using PyEnsign that calls the Finnhub API to retrieve real-time prices and a subscriber that receives these prices and builds a real-time chart using Streamlit and Plotly.

You will need to get an API key from [FinnHub](https://finnhub.io).  Add the API key as an environment variable as follows:

```
export FINNHUB_API_KEY="your key here"
```

To use PyEnsign, create a free account on [rotational.app](https://rotational.app/).  You will need to do the following once you create an account:

- [Create a project.](https://youtu.be/VskNgAVMORQ)
- Add a topic called `trades` in the project.  Check out this [video](https://youtu.be/1XuVPl_Ki4U) on how to add a topic.
- [Generate API keys for your project.](https://youtu.be/KMejrUIouMw)

You will need to create and source the following environment variables using the API keys you just downloaded:

```
export ENSIGN_CLIENT_ID="your client id here"
export ENSIGN_CLIENT_SECRET="your client secret here"
```

## Steps to run the application

### Create a virtual environment

```
$ virtualenv venv
```

### Activate the virtual environment

```
$ source venv/bin/activate
```

### Install the required packages

```
$ pip install -r requirements.txt
```

### Run the application
```
$ streamlit run trades.py
```

This will launch a browser that displays a line chart.  Take a look at this [example](trades.mp4).

