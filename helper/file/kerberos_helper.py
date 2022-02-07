import os,sys
root_path = os.environ.get('PYTHONPATH',os.environ.get('JUPYTERHUB_ROOT_DIR'))+'/'
sys.path.insert(0, root_path)

import subprocess
import argparse
import getpass


'''
PRE-REQUISITE : 
1. Install kerberos client
sudo apt-get install krb5-user

2. set-up kerberos configuartion (Example)
sudo nano /etc/krb5.conf
    [libdefaults]
    default_realm = <REALM>
    dns_lookup_kdc = false
    dns_lookup_realm = false
    ticket_lifetime = 86400
    renew_lifetime = 604800
    forwardable = true
    default_tgs_enctypes = rc4-hmac
    default_tkt_enctypes = rc4-hmac
    permitted_enctypes = rc4-hmac
    udp_preference_limit = 1
    kdc_timeout = 3000
    [realms]
    <REALM> = {
    kdc = <realm>
    admin_server = <realm>
    default_domain = <realm>
    }
    [domain_realm]
    tdembigdata.local = <REALM>
'''

def authen_krb(user, password):
    status = krbauth(user, password)
    if self == False:
        raise Exception("Kerberos Authentication Fail")


def krbauth(username, password, realm = None):
    if realm != None:
        username = '{}@{}'.format(username, realm)
        
    cmd = ['kinit', username]
    success = subprocess.run(cmd, input=password.encode(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
    return not bool(success)


def log(message, quiet):
    if not quiet:
        print('[*] {}'.format(message))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--username', help='Kerberos username', type=str)
    parser.add_argument('-p', '--password', help='Kerberos password', type=str)
    parser.add_argument('-r', '--realm', help='Kerberos REALM', type=str)
    parser.add_argument('-q', '--quiet', help='Quiet mode', action='store_true')
    args = parser.parse_args()

    username = args.username or input('[*] Kerberos username: ')
    password = args.password or getpass.getpass('[*] Kerberos password: ')
    if args.realm:
        username = '{}@{}'.format(username, args.realm)

    log('Logging in as {}'.format(username), args.quiet)
    authen_krb(user, password)