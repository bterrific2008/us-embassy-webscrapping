import logging
import os
import tarfile
import threading
import time
from queue import Queue

from google.cloud import storage

from webscrapper import embassy, state

BUF_SIZE = 100  # total number of iterations/items to process
WRITE_JOB = 0
READ_JOB = 1

BUCKET_NAME = os.environ['GCP_BUCKET']

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    logging.info(
        f"[UPLOAD BLOB] For bucket {bucket_name}, uploading {source_file_name} to {destination_blob_name}"
    )

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logging.info(
        "[UPLOAD BLOB] File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


def make_tarfile(output_filename, source_dir):
    logging.info(f"[MAKE TAR] Creating tarfile {output_filename} from {source_dir}")
    with tarfile.open(output_filename, "w:gz") as tar_handle:
        tar_handle.add(source_dir, arcname=os.path.basename(source_dir))


class Embassy_Consumer:
    def __init__(self, queue, my_id):
        logging.info(f"[CONSUMER] Embassy Consumer {my_id} created")
        self.queue = queue
        self.id = my_id

    def write_post_job(self, embassy_post_job: dict):
        order = embassy_post_job["order"]
        post = embassy_post_job["post"]
        country_name = embassy_post_job["country_name"]
        data_path = embassy_post_job["data_path"]

        logging.info(
            f"[CONSUMER] Embassy Consumer {self.id} running job {order} for {country_name}"
        )

        file_name, file_path = embassy.read_post_to_file(
            country_name,
            post,
            data_path,
            order,
        )

        logging.info(
            f"[CONSUMER] Embassy Consumer {self.id} finished analysis for {country_name} {order}"
        )

        return file_name, file_path

    def get_post_job(self, embassy_post_job: dict):
        country_url = embassy_post_job["url"]
        embassy_page_number = embassy_post_job["page_number"]
        data_path = embassy_post_job["data_path"]
        country_name = embassy_post_job["country_name"]
        post_count = (embassy_page_number - 1) * 10 + 1

        embassy_posts, _ = embassy.get_embassy_posts(
            country_url, page_number=embassy_page_number, page_count=10
        )
        logging.info(
            f"[CONSUMER] retrieved {len(embassy_posts)} from {country_url} on page {embassy_page_number}"
        )

        logging.info(f"[CONSUMER] Adding {len(embassy_posts)} jobs to queue")
        for post_json in embassy_posts:
            self.queue.put(
                {
                    "type": WRITE_JOB,
                    "content": {
                        "order": post_count,
                        "post": post_json,
                        "country_name": country_name,
                        "data_path": data_path,
                    },
                }
            )
            post_count += 1


    def run(self):
        logging.info(f"[CONSUMER] Embassy Consumer {self.id} created")

        if self.queue.empty():
            time.sleep(10)
        while not self.queue.empty():
            start_time = time.time()
            embassy_post_job = self.queue.get()
            embassy_post_job_type = embassy_post_job["type"]
            embassy_post_job_content = embassy_post_job["content"]

            if embassy_post_job_type == WRITE_JOB:
                post_file_name, post_file_path = self.write_post_job(embassy_post_job_content)
                if post_file_name and post_file_path:
                    upload_blob(
                        bucket_name=BUCKET_NAME,
                        source_file_name=post_file_path,
                        destination_blob_name=f"datasets/us_embassy_scrape/{post_file_name}",
                    )
            elif embassy_post_job_type == READ_JOB:
                self.get_post_job(embassy_post_job_content)


            self.queue.task_done()

            logging.info(
                f"[CONSUMER] Embassy Consumer {self.id} finished job after {time.time()-start_time}"
            )

            time.sleep(5)


def main():
    logging.info("Started Webscrapping Run")
    q = Queue()

    data_directory = os.path.join(os.getcwd(), "data")
    country_url_list = list(state.load_embassies(filepath=data_directory).items())
    """country_idx = {
        'start': 36,
        'end': 45,
    }"""
    country_idx = {
        "start": 49,
        "end": 100,
    }

    for country_name, country_url in country_url_list[
        country_idx["start"] : country_idx["end"]
    ]:
        logging.info(f"[QUEUE] Adding jobs for {country_name}")

        _, embassy_total_page_number_str = embassy.get_embassy_posts(
            country_url, page_number=1, page_count=10
        )
        embassy_total_page_number = int(embassy_total_page_number_str)

        logging.info(f"[QUEUE] Embassy has {embassy_total_page_number} pages")

        # create data directory location
        data_path = os.path.join(data_directory, country_name)
        if not os.path.exists(data_path):
            os.makedirs(data_path)
            logging.info(f"[QUEUE] created dir {data_path}")

        for page_number in range(2):
            q.put(
                {
                    "type": READ_JOB,
                    "content": {
                        "url": country_url,
                        "page_number": page_number + 1,
                        "data_path": data_path,
                        "country_name": country_name,
                    },
                }
            )
            if i%5 == 0:
                logging.info(f"\t[QUEUE] Added {i} jobs")

    logging.info("[MAIN] Creating Embassy Consumer 1")
    consumer_1 = Embassy_Consumer(q, 1)
    consumer_thread_1 = threading.Thread(target=consumer_1.run)

    logging.info("[MAIN] Creating Embassy Consumer 2")
    consumer_2 = Embassy_Consumer(q, 2)
    consumer_thread_2 = threading.Thread(target=consumer_2.run)

    logging.info("[MAIN] Creating Embassy Consumer 3")
    consumer_3 = Embassy_Consumer(q, 3)
    consumer_thread_3 = threading.Thread(target=consumer_3.run)

    consumer_thread_1.start()
    consumer_thread_2.start()
    consumer_thread_3.start()

    consumer_thread_1.join()
    consumer_thread_2.join()
    consumer_thread_3.join()
    q.join()


if __name__ == "__main__":
    # create the logger
    logger = logging.getLogger("webscrapping_logger")
    logger.setLevel(logging.DEBUG)

    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create and set formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    # set logger basic config
    logging.basicConfig(filename="webscrapping_run_4.log", level=logging.DEBUG)

    # recording time
    start_code = time.time()
    main()
    logging.info(f"Total runtime {time.time() - start_code}")
