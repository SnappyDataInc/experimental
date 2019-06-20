snappy-ansible-deploy
=======

## Notes
  * This version of deploying Snappy with Ansible assumes a RHEL 6.x or 7.x host.
  * The installation is only for a single host, residing on the same host where Ansible is installed

## Instructions
1. Install Ansible  
`sudo rpm -i epel-release-latest-7.noarch.rpm`  
`sudo yum update`  
`sudo yum install ansible`
2. Create Ansible group and role  
`sudo useradd ansible`  
`sudo groupadd ansible`  
`sudo usermod -aG ansible ansible`
3. Add Ansible to wheel group for passwordless sudo  
`sudo visudo`  
* Find the lines in the file that grant sudo access to users in the group wheel when enabled.  
`## Allows people in group wheel to run all commands`  
`#%wheel         ALL=(ALL)       ALL`  
* Remove the comment character at the start of the line
* Save changes and exit editor
4. Add ansible to wheel group  
`sudo usermod -aG wheel ansible`  
5. Switch to ansible user  
`sudo su ansible`  
6. Create directory to store Ansible files/playbooks  
`sudo mkdir /opt/ansible`
7. Download the ansible directory from this snappy-ansible-deploy directory and place the contents it in /opt/ansible  
8. Download jdk-8u171-linux-x64.tar.gz and put in /opt/ansible/roles/snappydata/files  
9. Download SnappyData and put in /opt/ansible/roles/snappydata/files
10. Update /opt/ansible/roles/snappydata/vars/main.yml to reflect Snappy version
* set snappy_verson variable equal to name of the Snappy download file, without the .tar.gz extension
11. Set permissions on the ansible folder and contents  
`sudo chown -R ansible.ansible /opt/ansible`  
12. Run the Ansible playbook to install snappy on your localhost
`cd /opt/ansible/snappydata/tasks`  
`ansible-playbook -i /opt/ansible/inventories/localhost/ -s /opt/ansible/roles/snappydata/tasks/deploy_snappy.yml --extra-vars "environment_name=localhost"`  
12. Start Snappy cluster  
`cd /opt/snappydata/sbin`  
`sudo su snappydata`  
`./snappy-start-all.sh`  
