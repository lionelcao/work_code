#! /bin/bash
###############################################################
######################### Set Variables #######################
###############################################################
set -e
export SYS="Simba"
export DBNM="capman2"
export TBLNM="cpmn_src_tgt_mp"
export DATE=$(date +"%Y%m%d")
export DATATC=/x/home/dw_adm/etl/bteq/dat/${TBLNM}/${SYS}.${DATE}.dat
export PSW=`cat /x/home/dw_adm/etl/.logon/b_capman_batch.logon`
export PSW1=`cat /x/home/dw_adm/etl/.logon/${DBNM}.logon`
if [ -f ${DATATC} ]; then
rm ${DATATC}
echo "Removed existed data file"
fi
set +e
###############################################################
###                    Extract Data                         ###
###############################################################
set -e
bteq <<ZBTEQ
.SET SESSION TRANS BTET;
.SET titledashes off;
.SET recordmode off;
.logon ${SYS}/b_capman_batch,${PSW};
.SET SEPARATOR ',';
.set width 500;
.EXPORT FILE = ${DATATC}
sel  '${SYS}' ||','||
     SCRPTNAME||','||
        TGT_DB||','||
       TGT_TBL||','||
        SRC_DB||','||
       SRC_TBL||','||
       JOBNAME||','||
  PRNTCNTRNAME||','||
   TOPCNTRNAME
from ddm_capman_t.CPMN_SRC_TGT_MP
;
.EXPORT RESET;
.LOGOFF;
.EXIT;
ZBTEQ
set +e
echo "Successfully extracted data from Teradata!"

sed -i '1d' ${DATATC}

###############################################################
###                    Load Data                            ###
###############################################################
set -e
mysql -h ******************* -P3115 -u${DBNM} -p${PSW1} <<EOF
  use ${DBNM};
  load data local infile '${DATATC}' into table ${DBNM}.${TBLNM} fields terminated by ',' ignore 1 lines;
EOF
echo "Successfully load data into MySQL!"
echo "Mission Complete!"
set +e
