import pika, json

#upload file to mongodb with gridfs then put msg in rabbitmq queue downstream service pull msg from queue can process upload by pulling from monogdb async flow between gateway and converter
def upload(f,fs,channel,access):
    try:
        fid=fs.put(f)
    except Exception as err:
        return "internal server error", 500
    message = {
        "video_fid": str(fid),
        "mp3_fid": None,
        "username": access["username"],
    }

    try:
        channel.basic_publish(
            exchange="",
            routing_key="video", 
            body=json.dumps(message), #python to json format
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSITENT_DELIVERY_MODE),
        ) #if pod restarts queue/messages should be retained
    except Exception as err:
        fs.delete(fid) #delete file from monogodb as no msg for file but file exists in mongo
        return "internal server error", 500

