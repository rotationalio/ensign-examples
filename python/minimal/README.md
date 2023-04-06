# Python Minimal Usage Example

This mini-project will help to test that your API keys are working as expected -- from Python!
*Note: this assumes that you've already registered for API keys -- if you still need to do that, first go to [rotational.app](https://rotational.app/), create an account, and then generate and download your keys.*

## Requirements

Install the Ensign Python SDK with pip.

```
$ pip install pyensign
```

Next, go to your bash profile and create two new environment variables:

```
export ENSIGN_CLIENT_ID="your client id here"
export ENSIGN_CLIENT_SECRET="your client secret here"
```

Now `source` your profile and run the script with Python.

```
$ python main.py
```

If you encounter a problem using the SDK (or have a cool feature suggestion), feel free to open a [GitHub issue](https://github.com/rotationalio/pyensign/issues).