import logging

from .servicex_databinder import DataBinder # NOQA

__version__ = '0.5.0'

logging.basicConfig(format="%(levelname)s - %(message)s")
logging.getLogger(__name__).setLevel(logging.INFO)
