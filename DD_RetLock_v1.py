#!/usr/bin/python3
#
# Script: DD_RetLock.py
# Mandatory Arguments:      Configuration File Name 
#
# Output:                   none                    => All output is going to log file
# Exit:                     0                       => All good
#====================Versions====================
#====Version========================Comments=====
# 1.0   =>  Intial Script
#
##################################################
import argparse
from paramiko import SSHClient,AutoAddPolicy
import xml.etree.ElementTree as et
import logging
import sys
import os
import time
import datetime
import calendar
import subprocess
#
# Subs
#
########################################
# Sub: delExpiredMtree
# Purpose: finds and deletes expired retention locked mtrees
########################################
def delExpiredMtree(DDSystem):
    logger.debug("In delExpiredMtree, got DDSystem: %s", DDSystem)
    lockedMtrees = []
    loopCount1 = 0
    loopCount2 = 0
    tmp_t = datetime.datetime.now()
    today = tmp_t.strftime('%Y-%m-%d %H:%M:%S')
    curEpochStamp = calendar.timegm(time.strptime(str(today), '%Y-%m-%d %H:%M:%S'))
    logger.info("Current Epoch Timestamp = %i",curEpochStamp)
    logger.debug("Issuing mtree list to Data Domain")
    dd_in,dd_out,dd_err = DDClient.exec_command('mtree list')
    rawStream = dd_out.read()
    # Splitting the raw stream at \n to get a list of mtree's
    splitStream = rawStream.split(b'\n')
    # Building the list of retention locked mtree's on the system
    while loopCount1 < len(splitStream):
        if "RetLock" in splitStream[loopCount1].decode('ascii'):
            lockedMtrees.append(splitStream[loopCount1].decode('ascii'))
        loopCount1 += 1
    while loopCount2 < len(lockedMtrees):
        tmp_workItem = " ".join(lockedMtrees[loopCount2].split())
        workItem = tmp_workItem.split()
        tmpSplitItem = workItem[0].split('_')
        MtreeExpire = int(tmpSplitItem[2])           # Getting the expiration timestamp of the mtree
        if MtreeExpire < curEpochStamp:
            if "D" in workItem[2]:                      # Mtree has already been deleted
                logger.info ("Mtree %s has already been deleted", workItem[0])
            else:
                logger.info ("Mtree %s is expired and present", workItem[0])
                logger.info ("Working on mtree name: %s ... going to delete the mtree",workItem[0])
                prepareMtreeDelete(workItem[0], DDSystem)
                tmp_in,tmp_out,tmp_err = DDClient.exec_command('mtree delete ' + str(workItem[0]))
                tmp_in.write("yes\n")
        else:
            logger.info ("Mtree %s has not expired yet...",workItem[0])
        loopCount2 += 1
    return 0
########################################
# Sub: prepareMtreeDelete
# Purpose: mounts and deletes the data of the mtree in preparation for mtree deletion
# Input: Mtree Name
# Output: 0 if ok, non 0 if failure
########################################
def prepareMtreeDelete(mtreeName, DDSystem):
    logger.debug ("In prepareMtreeDelete, got mtreeName: %s | DDSystem: %s", mtreeName, DDSystem)
    proc = subprocess.Popen(["hostname"],stdout=subprocess.PIPE,shell=True)
    (output,error) = proc.communicate()
    hostname = output.decode('ascii').strip('\n')
    nfsOptions="no_root_squash"
    exportCreateCmd='nfs export create ' + str(mtreeName) + ' path ' + str(mtreeName) + ' clients ' + hostname + ' options ' + str(nfsOptions)
    exportDestroyCmd='nfs export destroy ' + str(mtreeName)
    logger.debug("Sending %s to DD", exportCreateCmd)
    dd_in,dd_out,dd_err = DDClient.exec_command(exportCreateCmd)
    logger.debug(f'DD_OUT: {dd_out.read()}')
    logger.debug(f'DD_ERR: {dd_err.read()}')
    mountCommand = "sudo /usr/sbin/mount.nfs " + str(DDSystem) + ":" + str(mtreeName) + " /mnt"
    umountCommand = "sudo /usr/bin/umount /mnt"
    rmCommand = "sudo /usr/bin/rm -rf /mnt/*"
    logger.debug("Sending mount command: %s", mountCommand)
    os.system(mountCommand)
    logger.debug("Sending delete command: %s", rmCommand)
    os.system(rmCommand)
    logger.debug("Sending umount command: %s", umountCommand)
    os.system(umountCommand)
    dd_in,dd_out,dd_err = DDClient.exec_command(exportDestroyCmd)
    logger.debug(f'DD_OUT: {dd_out.read()}')
    logger.debug(f'DD_ERR: {dd_err.read()}')
    return 0
########################################
# Sub: readConfig
# Purpose: Parses a xml configuration file
# Input: cfgFileName
########################################
def readConfig(cfgFileName):
    logger.debug("In readConfig, got: cfgFileName: %s", cfgFileName)
    sectList = ['SSHKey','DDUser','DDSystem','DDMtree','OracleServer','Retention']
    elemList = []
    counter1 = 0
    counter2 = 0
    global SSHKeyName
    global DDUserName
    global DDSystemName
    global DDMtreeName
    OraServerList = []
    global RetName
    logger.info("Checking for existence of configuration file: %s", cfgFileName)
    if os.path.exists(cfgFileName) and os.path.isfile(cfgFileName):
        logger.debug("File: %s exists and is a file",cfgFileName)
        logger.info("Found configuration file %s ...",cfgFileName)
        cfg = et.parse(cfgFileName)
    else:
        logger.error("File: %s not found...no configuration available, exiting...",cfgFileName)
        sys.exit(2)
    # Building the working configuration
    logger.debug("Before while loop ==> Current Counters -- counter1: %i -- counter2: %i", counter1, counter2)
    while counter1 < len(sectList):
        logger.debug("In while loop ==> Counters -- counter1: %i -- counter2: %i", counter1, counter2)
        logger.debug("working on sectList: %s",sectList[counter1])
        for element in cfg.iterfind(sectList[counter1]):
            logger.debug("working on element: %s -- list is at; %s",element,sectList[counter1])
            if sectList[counter1] == 'SSHKey':
                logger.debug("in readConfig read loop...got: %s", sectList[counter1])
                tmp = element.attrib
                SSHKeyName = tmp['name']
                logger.info ("Setting ssh key to: %s", SSHKeyName)
            elif sectList[counter1] == 'DDUser':
                logger.debug("in readConfig read loop...got: %s", sectList[counter1])
                tmp = element.attrib
                DDUserName = tmp['name']
                logger.info ("Setting DD user name to: %s", DDUserName)
            elif sectList[counter1] == 'DDSystem':
                logger.debug("in readConfig read loop...got: %s", sectList[counter1])
                tmp = element.attrib
                DDSystemName = tmp['name']
                logger.info("Setting DD system name to: %s", DDSystemName)
            elif sectList[counter1] == 'DDMtree':
                logger.debug("in readConfig read loop...got: %s", sectList[counter1])
                tmp = element.attrib
                DDMtreeName = tmp['name']
                logger.info("Setting DD mtree name to: %s", DDMtreeName)
            elif sectList[counter1] == 'OracleServer':
                logger.debug("in readConfig read loop...got: %s", sectList[counter1])
                OraServerList.append(element.attrib['name'])
                logger.info("Setting server %i name to: %s", counter2, OraServerList[counter2])
                counter2 += 1
            elif sectList[counter1] == 'Retention':
                logger.debug("in readConfig read loop...got: %s", sectList[counter1])
                tmp = element.attrib
                RetName = tmp['name']
                logger.info("Setting retention to: %s", RetName)
        counter1 += 1
    return(OraServerList)
########################################
# Sub: ssh_connect
# Purpose: Opening ssh connection to Data Domain
# Input: Data Domain Name
########################################
def connectDD(DDRName,DDRKey,DDRUser):
    logger.debug("In connectDD, got DDRName: %s | DDRUser: %s | DDRKey: %s", DDRName, DDRUser, DDRKey) 
    global DDClient
    DDClient = SSHClient()
    DDClient.set_missing_host_key_policy(AutoAddPolicy())         # automatically add the verification if its not known. 
    connection = DDClient.connect(DDRName,username=DDRUser,key_filename=DDRKey)
    if connection is None:
        logger.info("Connected to DDR: %s with User %s", DDRName, DDRUser)
    else:
        logger.info("Connection to DDR %s failed", DDRName)
        sys.exit(1)
    return 0
########################################
# Sub: createFastCopy
# Purpose: create a fastcopy of mtree
########################################
def createFastCopy(SrcMtreeName,DstMtreeName,OracleServers):
    logger.debug("In createFastCopy, got SrcMtreeName: %s | DstMtreeName: %s | OracleServers: %s", SrcMtreeName, DstMtreeName, OracleServers)
    counter1 = 0
    while counter1 < len(OracleServers):
        logger.info("Executing fastcopy of %s/%s to %s/%s",SrcMtreeName,OracleServers[counter1],DstMtreeName,OracleServers[counter1])
        dd_in,dd_out,dd_err = DDClient.exec_command('filesys fastcopy source ' + str(SrcMtreeName) + '/' + str(OracleServers[counter1]) + ' destination ' + str(DstMtreeName) + '/' + str(OracleServers[counter1]))
        logger.debug(f'DD_OUT: {dd_out.read()}')
        logger.debug(f'DD_ERR: {dd_err.read()}')
        counter1 += 1
    return 0
########################################
# Sub: createRetentionLock
# Purpose: create a retention locked mtree as the target for the fastcopy
# Input: SourceMtree, Expiration, ssh_handle
# Output: TgtMtreeName
########################################
def createRetLockMtree(SrcMtreeName,Expiration):
    logger.debug("In createRetLockMtree, got SrcMtreeName: %s | Expiration: %s", SrcMtreeName, Expiration)
    AutoLckDelay="10min"
    expstamp = Expiration.split("-")
    AutoLckRetention=str(expstamp[0]) + str(expstamp[1])
    logger.info("Setting auto lock retention to: %s",AutoLckRetention)        
    tmp_today = datetime.datetime.now()
    today = tmp_today.strftime('%Y-%m-%d %H:%M:%S')
    cur_epoch = calendar.timegm(time.strptime(str(today), '%Y-%m-%d %H:%M:%S'))
    tmp_expire = tmp_today + datetime.timedelta(days=int(expstamp[0]))
    expire = tmp_expire.strftime('%Y-%m-%d %H:%M:%S')
    exp_epoch = calendar.timegm(time.strptime(str(expire), '%Y-%m-%d %H:%M:%S'))
    logger.debug("Got %d as current tstamp and %d as expire tStamp", int(cur_epoch), int(exp_epoch))
    logger.info("Creating target mtree from mtree name: %s", SrcMtreeName)
    TgtMtreeName = SrcMtreeName + "_RetLock_" + str(exp_epoch)
    logger.info("Creating target mtree name: %s", TgtMtreeName)
    dd_in,dd_out,dd_err = DDClient.exec_command('mtree create ' + str(TgtMtreeName))
    logger.debug(f'DD_OUT: {dd_out.read()}')
    logger.debug(f'DD_ERR: G{dd_err.read()}')
    logger.info("Enabling Retention Lock on mtree: %s", TgtMtreeName)
    dd_in,dd_out,dd_err = DDClient.exec_command('mtree retention-lock enable mode governance mtree ' + str(TgtMtreeName))
    logger.debug(f'DD_OUT: {dd_out.read()}')
    logger.debug(f'DD_ERR: {dd_err.read()}')
    logger.info("Enabling auto lock on %s and setting auto retention period to %s ...", TgtMtreeName, Expiration)
    dd_in,dd_out,dd_err = DDClient.exec_command('mtree retention-lock set automatic-retention-period ' + str(AutoLckRetention) + ' mtree ' + str(TgtMtreeName))
    logger.debug("Sending answer to DD......")
    dd_in.write('yes\n')
    logger.debug(f'DD_OUT: {dd_out.read()}')
    logger.debug(f'DD_ERR: {dd_err.read()}')
    dd_in.flush()
    time.sleep(2.0)
    logger.debug(f'DD_OUT: {dd_out.read()}')
    logger.debug(f'DD_ERR: {dd_err.read()}')
    logger.info("Setting automatic lock delay to %s ...", AutoLckDelay)
    dd_in,dd_out,dd_err = DDClient.exec_command('mtree retention-lock set automatic-lock-delay ' + str(AutoLckDelay) + ' mtree ' + str(TgtMtreeName))
    logger.debug(f'DD_OUT: {dd_out.read()}')
    logger.debug(f'DD_ERR: {dd_err.read()}')
    return TgtMtreeName
########################################
# Sub: disconnectDD
# Purpose: closes SSH handle
# Input: ssh handle
# Output: 0 => OK
#         1 => Not ok, but not to worry :-)
########################################
def disconnectDD():
    logger.debug("In disconnectDD")
    logger.info("Closing ssh connection to Data Domain")
    DDClient.close()
    return 0
## Main
#
Log_Format = "%(asctime)s [%(levelname)s] -- %(message)s"
logging.basicConfig(filename = "./test.log", filemode = "a", format = Log_Format, level = logging.INFO)
logger=logging.getLogger()
runDate = datetime.date.today()
logger.info("========== Retention Lock Script Start =============")
logger.info("Starting Date: %s", runDate.strftime("%d-%m-%Y"))
parser = argparse.ArgumentParser(description='Process Mandatory and Optional Arguments')
parser.add_argument("Configfile", help="XML configuration file name")
arguments = parser.parse_args()
logger.info("Configfile name: %s", arguments.Configfile)
logger.info("Reading config file")
OraServers=readConfig(arguments.Configfile)
logger.debug("======> Got key: %s",SSHKeyName)
logger.debug("======> Got DD User: %s",DDUserName)
logger.debug("======> Got DD System: %s",DDSystemName)
logger.debug("======> Got DD Mtree: %s",DDMtreeName)
logger.debug("======> Got DD Retention: %s",RetName)
logger.debug("======> Got %i Oracle Systems", len(OraServers))
z=0
while z < len(OraServers):
    logger.debug("==========> Oracle Server no. %i is %s",z,OraServers[z])
    z += 1
logger.debug("Jumping to connectDD()")
connectDD(DDSystemName,SSHKeyName,DDUserName)
logger.debug("returned from connectDD() jumping to createRetLockMtree")
TargetMtree = createRetLockMtree(DDMtreeName,RetName)
logger.debug("Returned from creating ret locked mtree (name: %s)...going to fastcopy now", TargetMtree)
FCRet = createFastCopy(DDMtreeName,TargetMtree,OraServers)
logger.debug("Returned from fast copy, jumping to delExpiredMtree")
DelRet = delExpiredMtree(DDSystemName)
disconnectDD()
sys.exit(0)
