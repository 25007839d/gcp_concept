import logging


if __name__=="__main__":

     pcm_logger = logging.getLogger()
     pcm_logger.setLevel(logging.DEBUG)
     pcm = logging.FileHandler(f"gs://pcm_dev-ingestion/log/pcm.log")
     pcm.setLevel(logging.DEBUG)
     pcm_formater = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
     pcm.setFormatter(pcm_formater)
     pcm_logger.addHandler(pcm)


     logger = logging.getLogger(__name__)
     logger.setLevel(logging.DEBUG)
     ch = logging.FileHandler(f"gs://pcm_dev-ingestion/log/pcm1.log")
     ch.setLevel(logging.DEBUG)
     formatter = logging.Formatter('%(message)s')
     ch.setFormatter(formatter)
     logger.addHandler(ch)




     logger.info("hellow")
     pcm_logger.info("pcm logger")