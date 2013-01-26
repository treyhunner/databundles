'''
Created on Jan 17, 2013

@author: eric
'''
import unittest


class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_basic(self):
        import ckanclient
        
        uid = 'foobottom'
        
        # Instantiate the CKAN client.
        ckan = ckanclient.CkanClient(base_location='http://data.vagrd.cnshost.net/api',
                                     api_key='33441d7b-45aa-4306-807f-871cb502484a')

        package_list = ckan.package_register_get()
        print package_list

        package_entity = ckan.package_entity_get(uid)
     
        import pprint
        pprint.pprint(package_entity)

        import glob

        for f in glob.glob("/Volumes/DataLibrary/crime/months/*.csv"):
            parts = f.split('/').pop().strip('.csv').split('-')
            resource_tag = "{0}-{1:02d}".format(parts[0],int(parts[1]))
            print "Start ", resource_tag

            url,err = ckan.upload_file(f) #@UnusedVariable
            print "!!!",url

            r = ckan.add_package_resource(uid, f, 
                                          resource_type = 'data',
                                          description=resource_tag, 
                                          tags = resource_tag,
                                          author  = 1,
                                          creator = 2,
                                          dataset = 3, 
                                          subset = 4
                                          )
            
            pprint.pprint(r)
           
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()