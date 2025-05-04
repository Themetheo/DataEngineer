import logging

# logging.DEBUG: Detailed information, typically only of interest to a developer trying to diagnose a problem.

# logging.INFO : Confirmation that things are working as expected.

# logging.WARNING :An indication that something unexpected happened, or that a problem might occur in the near future (e.g. ‘disk space low’). The software is still working as expected.

# logging.ERROR : Due to a more serious problem, the software has not been able to perform some function.

#logging.CRITICAL : A serious error, indicating that the program itself may be unable to continue running.
print("123")
logging.basicConfig(filename='test.log',level=logging.DEBUG
                    ,format='%(filename)s:%(asctime)s:%(levelname)s:%(message)s')
def add(x,y) :
    return x+y
x=4
y=5
result_add = add(x,y)
logging.debug('Add: {} + {} = {}'.format(x,y,result_add))