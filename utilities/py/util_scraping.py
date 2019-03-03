
import requests
import json

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}

def getURLJson(URL):
    """
    Parameters
    ----------
    URL: : str
        url path of resource to retrieve
        
    Returns
    -------
    json_data : json
        json object containing request results
    """
    
    json_data = requests.get(URL, headers=headers).json()

    return json_data
