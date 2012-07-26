'''
Created on Jul 24, 2012

@author: eric
'''

## {{{ http://code.activestate.com/recipes/52549/ (r3)
class curry:
    def __init__(self, fun, *args, **kwargs):
        self.fun = fun
        self.pending = args[:]
        self.kwargs = kwargs.copy()

    def __call__(self, *args, **kwargs):
        if kwargs and self.kwargs:
            kw = self.kwargs.copy()
            kw.update(kwargs)
        else:
            kw = kwargs or self.kwargs

        return self.fun(*(self.pending + args), **kw)
## end of http://code.activestate.com/recipes/52549/ }}}
