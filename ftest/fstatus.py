import time
from fabric import Connection

ollie  = [
    '/home/ollie/nkolduno/VISC/fesom2/work_visc_control/',
    '/home/ollie/nkolduno/VISC/fesom_new_visc/work_option_1/',
    '/home/ollie/nkolduno/VISC/fesom_new_visc/work_option_2/',
    '/home/ollie/nkolduno/VISC/fesom_new_visc/work_option_3/',
    '/home/ollie/nkolduno/VISC/fesom_new_visc/work_option_4/',
    '/home/ollie/nkolduno/VISC/fesom_new_visc/work_option_5_1/',
    '/home/ollie/nkolduno/VISC/fesom_new_visc/work_option_6/',
    '/home/ollie/nkolduno/VISC/fesom_new_visc/work_option_7/',
    '/home/ollie/nkolduno/VISC/fesom2/work_pi_standard/',
]

request = 'conda activate pyfesom2;'
for exper in ollie:
    request = request +'report '+ exper + ';'
print(request)

c = Connection('ollie1', user='nkolduno',
               connect_kwargs={"key_filename": "/Users/koldunovn/.ssh/id_rsa_ole"})

result = c.run(request,  echo=True)

c.close()
