"""http exceptions

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


class HttpException(Exception): pass
class BadRequest(HttpException): 
    code = 400
    http_message = 'Bad Request'
    def __init__(self, message): 
        super(HttpException, self).__init__(self.http_message+":"+str(message))
class NotAuthorized(HttpException):     
    code = 401
    http_message = 'Not Authorized'
    def __init__(self, message): 
        super(HttpException, self).__init__(self.http_message+":"+str(message))
class NotFound(HttpException): 
    code = 404
    http_message = 'Not Found'
    def __init__(self, message): 
        super(HttpException, self).__init__(self.http_message+":"+str(message))
class MethodNotAllowed(HttpException): 
    code = 405
    http_message = 'Method Not Allowed'
    def __init__(self, message): 
        super(HttpException, self).__init__(self.http_message+":"+str(message))

class Conflict(HttpException):
    code = 409
    http_message = 'Conflict'
    def __init__(self, message): 
        super(HttpException, self).__init__(self.http_message+":"+str(message))
class TooLarge(HttpException):
    code = 413
    http_message = 'Request entity too large'
    def __init__(self, message): 
        super(HttpException, self).__init__(self.http_message+":"+str(message)) 
        
  
class InternalError(HttpException):
    code = 500
    http_message = 'Internal Server Error'
    def __init__(self, message): 
        super(HttpException, self).__init__(self.http_message+":"+str(message)) 
        
exceptions = {
    400: BadRequest,
    401: NotAuthorized,
    404: NotFound,
    405: MethodNotAllowed,
    409: Conflict,
    413: TooLarge,
    500: InternalError
}

    
def get_exception(code):
    """Return an exception class based on its code"""
    try:
        return exceptions[int(code)]
    except:
        return None
    