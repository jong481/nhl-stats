
import logging

class UtilLogging:
    """
    Class used to represent a logging object to write to a specific file
    """
    
    def __init__(self, log_file, log_name):
        """
        Parameters
        ----------
        log_file : str
            file name of desired log file
        log_name : str
            name for logging object
        """
            
        self.log_file = log_file
        self.log_name = log_name
                
    def initiateLogger(self):
        """Setup logger object with specified file name and logger name
        
        Returns
        ----------
        logger : logger object
            logger object with specified file name and logger name
        """
        
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler = logging.FileHandler(self.log_file, mode='a')
        handler.setFormatter(formatter)

        logger = logging.getLogger(self.log_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        
        return logger
