import requests
import json


def getResponse(request):
    request_args=request.args
    print("request_json",request_args)
    print("request_args miner_id",request_args['miner_id'])

    try:
        return_me={"image":"0x68747470733a2f2f697066732e696f2f697066732f516d5358416257356b716e3259777435444c336857354d736a654b4a4839724c654c6b51733362527579547871313f66696c656e616d653d73756e2d636861696e6c696e6b2e676966"}
        return return_me
    except Exception as e:
        print(e)
        return (e, 500)
