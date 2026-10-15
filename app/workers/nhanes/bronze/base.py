from celery import Task
from google.cloud import storage

from app.config.settings import settings
from app.utils.logger import get_logger
from gcp import storage_client
from app.utils.retry import create_retry_session
import time
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout

logger = get_logger(__name__)


class BronzeTask(Task):
    abstract = True

    def __init__(self):
        self.bucket = storage_client.bucket(settings.GCP_BUCKET_NAME)
        self.base_url = settings.NHANES_BASE_URL
        self._logger = get_logger(self.__class__.__name__)
        self.bucket_path = settings.BRONZE_PATH
        self.bucket_name = settings.GCP_BUCKET_NAME

        self.session = create_retry_session()


    def log_info(self, msg: str):
        self._logger.info(msg)

    def fetch_with_retry(self, url, max_attempts=3, initial_delay=2):
        session = self.session  # Fixing the NameError by using self.session
        for attempt in range(1, max_attempts + 1):
            try:
                self.log_info(f" Attempt {attempt}/{max_attempts}: Fetching {url}")
                
                response = session.get(
                    url, 
                    timeout=(50, 80),  # (connect_timeout, read_timeout)
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                )
                
                response.raise_for_status()
                
                # Validate response has content
                if not response.text or len(response.text.strip()) == 0:
                    raise ValueError("Empty response received")
                
                self.log_info(f" Successfully fetched URL on attempt {attempt}")
                return response
                
            except Timeout as e:
                self.log_info(f" Timeout error on attempt {attempt}: {str(e)}")
                
            except ConnectionError as e:
                self.log_info(f" Connection error on attempt {attempt}: {str(e)}")
                
            except HTTPError as e:
                if e.response and 400 <= e.response.status_code < 500:
                    self._logger.error(
                        f" Client error {e.response.status_code} for URL {url}",
                        exc_info=True
                    )
                    return None
                    
            except RequestException as e:
                self.log_info(f" Request error on attempt {attempt}: {str(e)}")
                
            except ValueError as e:
                self.log_info(f" Invalid response on attempt {attempt}: {str(e)}")
                
            except Exception:
                self._logger.exception(
                    f" Unexpected error while fetching URL {url} (attempt {attempt})"
                )


            # Wait before retrying (exponential backoff)
            if attempt < max_attempts:
                delay = initial_delay * (2 ** (attempt - 1))
                self.log_info(f" Waiting {delay} seconds before retry...")
                time.sleep(delay)
        
        self._logger.error(f" Failed to fetch URL after {max_attempts} attempts: {url}")
        return None

    