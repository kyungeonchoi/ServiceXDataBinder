import logging

from .servicex_databinder import DataBinder

__version__ = '0.2.6'

logging.basicConfig(format="%(levelname)s - %(message)s")
logging.getLogger(__name__).setLevel(logging.INFO)