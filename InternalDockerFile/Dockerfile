FROM akintsakis/hermescomponents:latest
#ENV DEBIAN_FRONTEND noninteractive

WORKDIR /home/user

###uncompress and index pfam-A
#RUN cd ./pfamAhmmDatabase && gunzip *.gz && hmmpress Pfam-A.hmm && rm Pfam-A.hmm
###
#RUN apt-get update -y && apt-get install autossh -y
###setup ssh
# Regenerate SSH host keys. baseimage-docker does not contain any, so you
# have to do that yourself. You may also comment out this instruction; the
# init system will auto-generate one during boot.
RUN rm -f /etc/service/sshd/down && /etc/my_init.d/00_regen_ssh_host_keys.sh && echo 'Port updatePortNumber' >> /etc/ssh/sshd_config
ADD currentWorkflowGeneratedIdentityKey.pub /tmp/identity.pub
ADD currentWorkflowGeneratedIdentityKey /root/.ssh/identity
ADD identityToHosts /root/.ssh/identityToHosts
RUN chmod 400 /root/.ssh/identity
RUN cat /tmp/identity.pub >> /root/.ssh/authorized_keys && rm -f /tmp/identity.pub
RUN echo 'Host *' >> /root/.ssh/config
RUN echo 'StrictHostKeyChecking no' >> /root/.ssh/config
RUN echo 'IdentityFile /root/.ssh/identity' >> /root/.ssh/config
##setup ssh

RUN echo 'root:hermes' | chpasswd

#RUN echo 'echo akraio > /home/user/akraio' >> /home/user/testIt
#RUN echo 'screen -S pidtwo -d -m' >> /home/user/testIt
#RUN echo 'screen -S pidtwo -X stuff 'pidstat -U root -dl 1 > /home/user/pidStatLogFile'`echo -ne '\\015'`' >> /home/user/testIt


ENTRYPOINT service ssh restart && bash




