

def resolve_id(arg, bundle=None, library=None):
    '''resolve any of the many ways of identifying a partition or
    bundle into an ObjectNumber for a Dataset or Partition '''
    from identity import ObjectNumber, Identity


    if isinstance(arg, basestring):
        on = ObjectNumber.parse(arg)
    elif isinstance(arg, ObjectNumber):
        return arg
    elif isinstance(arg, Identity):
        if not arg.id_ and bundle is None:
            raise Exception("Identity does not have an id_ defined")
        elif not arg.id_ and bundle is not None:
            raise NotImplementedError("Database lookup for Identity Id via bundle  is not yet implemented")
        elif not arg.id_ and bundle is not None:
            raise NotImplementedError("Database lookup for Identity Id via library is not yet implemented")
        else:
            on = ObjectNumber.parse(arg.id_)
 
    else:
        # hope that is has an identity field
        on = ObjectNumber.parse(arg.identity.id_)
        
    return on
