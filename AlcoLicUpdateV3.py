import arcpy
import smtplib
import traceback
import datetime
import StringIO
import getpass
import os


#   settings
arcpy.env.workspace = r"Database Connections\SDE@Planning_CLUSTER.sde"
arcpy.env.overwriteOutput = True

#   data
db_conn = r"Database Connections\SDE@Planning_CLUSTER.sde"
ComPlus_BusiLic = r"Database Connections\COMMPLUS.sde\COMPLUS.WPB_ALL_BUSINESSLICENSES"
Planning_AlcoLic = r"Planning.SDE.WPB_GIS_ALCOHOL_LICENSES"
Fields_lessObjID = [Field.baseName.encode('ascii') for Field in arcpy.ListFields(Planning_AlcoLic) if Field.baseName
                    != 'OBJECTID']
query_layer = "ALCOLICENCE_QL"
alco_licence_poly = "Alco_licence_poly"
alco_license_points = "Alco_license_points"
alco_license = "AlcoholLicense_complus"
spatialref = arcpy.Describe(r"Database Connections\SDE@Planning_CLUSTER.sde\Planning.SDE.LandUsePlanning").\
                              spatialReference.exportToString()
Planning_Alcohol_License_fullpath = r"Database Connections\SDE@Planning_CLUSTER.sde\Planning.SDE." \
                                    r"WPB_GIS_ALCOHOL_LICENSES"
logfile = r"Q:\log_files\Alcohol_Licences\logfile.txt"
#   following query created with help from Planning Dept. For details on value meanings, ask them.
Sql_copytable = "CATEGORY IN  ('AAM','445310','424810','312130','424820','445310','722410','312120','312140') AND" \
                " STAT IN ('ACTIVE','PRINTED','HOLD')"

def main():
    try:
        # create sets of license numbers from Community Plus and corresponding table in GIS Cluster
        ComplusLicenses_set = set()
        PlanningLicenses_set = set()

        with arcpy.da.SearchCursor(ComPlus_BusiLic, 'LICENSE', Sql_copytable) as ComplusSC:
            for record in ComplusSC:
                ComplusLicenses_set.add(record[0].strip())

        with arcpy.da.SearchCursor(Planning_AlcoLic, 'LICENSE') as PlanAlcSC:
            for record in PlanAlcSC:
                PlanningLicenses_set.add(record[0].strip())

        #   Compare the two sets, perform set calculations to discover differences (if any)

        InComplus_NotInSDE = ComplusLicenses_set.difference(PlanningLicenses_set)
        InSDE_NotInComplus = PlanningLicenses_set.difference(ComplusLicenses_set)

        #   If InComplus_NotInSDE has members, then proceed to append record to GIS_ALCOHOL_LICENSES, and create a point
        #       fc of record and append to AlcoholLicense_complus
        #   If InSDE_NotInComplus has members, then proceed to delete records from Planning.SDE.WPB_GIS_ALCOHOL_LICENSES
        #       table and Planning.SDE.AlcoholLicense_complus feature class

        if len(InComplus_NotInSDE) == 0:
            print 'no new alcohol license found'
            with open(logfile, "a") as log:
                now = datetime.datetime.now().strftime("%m-%d-%Y")
                log.write("\n-----------------\n")
                log.write(now + " no new alcohol licenses found\n\n")

        else:
            print 'new alcohol license found in complus, adding'
            #   change list to a tuple (in preparation of creating a text string for the query). Delta is in unicode, need
            #   it in plain ascii text for query

            InComplus_NotInSDE_tuple = tuple({license.encode('ascii') for license in InComplus_NotInSDE})
            if len(InComplus_NotInSDE_tuple) == 1:
                sqlquery = "LICENSE = '{}'".format(InComplus_NotInSDE_tuple[0])
            else:
                sqlquery = "LICENSE IN {}".format(InComplus_NotInSDE_tuple)

            #   it doesnt like insert cursor, so make a temp table and append to that, then append to GIS_ALCOHOL_LICENSES

            TempTable = arcpy.CreateTable_management("in_memory", "TempTable", Planning_AlcoLic)

            with arcpy.da.SearchCursor(ComPlus_BusiLic, Fields_lessObjID, sqlquery) as sc:
                with arcpy.da.InsertCursor(TempTable, Fields_lessObjID) as ic:
                    for record in sc:
                        ic.insertRow(record)

            arcpy.Append_management(TempTable, Planning_AlcoLic)

            #   THe following block creates a Query Layer from a join between the new licenses identified earlier and the
            #   parcels in which they reside, saves the Query layer as a polygon fc...
            #   changes that to point fc, then appends the points to AlcoholLicense_complus

            sql = "SELECT PARCELS.[OBJECTID],[OWNPARCELID] AS PARCELS_PCN,[SRCREF],[OWNTYPE]," \
                  "[GISdata_GISADMIN_OwnerParcel_AR],[LASTUPDATE],[LASTEDITOR],[Shape],[PARCEL_ID] AS COMPLUS_PCN," \
                  "[BUSINESS_ID],[LICENSE],[CATEGORY],[CATEGORY_DESC],[STAT],[ISSUE],[EXPIRATION],[BUS_ENTITY_ID]," \
                  "[BUS_NAME],[BUS_PROD],[SERVICE],[ADRS1],[BUS_PHONE],[BUS_EMAIL]" \
                  " FROM [Planning].[sde].[PLANNINGPARCELS] PARCELS,[Planning].[sde].[WPB_GIS_ALCOHOL_LICENSES] ALCOLIC" \
                  " WHERE PARCELS.OWNPARCELID = ALCOLIC.PARCEL_ID AND {}".format(sqlquery)

            arcpy.MakeQueryLayer_management(input_database=db_conn, out_layer_name=query_layer, query=sql,
                                            oid_fields="OBJECTID", shape_type="POLYGON", srid="2881",
                                            spatial_reference=spatialref)
            arcpy.management.CopyFeatures(query_layer, alco_licence_poly, None, None, None, None)
            arcpy.FeatureToPoint_management(alco_licence_poly, alco_license_points, "INSIDE")
            arcpy.Append_management(alco_license_points, alco_license)

            #   Create the alert email text. Uses StringIO to create a string treated as a file for formatting purposes

            TT_fieldnames =['PARCEL_ID', 'LICENSE', 'BUS_NAME', 'ADRS1']
            string_obj = StringIO.StringIO()
            with arcpy.da.SearchCursor(TempTable, TT_fieldnames) as TTSC:
                for row in TTSC:
                    string_obj.write('\t'.join(row))
                    string_obj.write('\n\n')

            report = string_obj.getvalue()

            sendMail('Alcohol License report',
                     # 'jssawyer@wpb.org',
                     ["cdglass@wpb.org", "jssawyer@wpb.org"],
                     "These have been added to AlcoholLicense_complus:\n\nPCN\t\t\tLicense\t\t"
                        "Business Name\t\tAddress",
                     report)

            with open(logfile, "a") as log:
                now = datetime.datetime.now().strftime("%m-%d-%Y")
                log.write("\n------------------------------------------\n\n")
                log.write(now)
                log.write('\n')
                log.write(report)
                log.write("\n")

        #   This section will delete from alcohol_license_complus and Planning.SDE.WPB_GIS_ALCOHOL_LICENSES any records that
        #   exists in Planning SDE but not in Complus (probably due to status change in complus)

        if len(InSDE_NotInComplus) == 0:
            print 'All licenses in SDE found in Commplus, no deletions needed'
        else:
            print 'Licenses found in SDE that dne is Commplus, deleting from SDE'
            InSDE_query_tup = tuple({record.encode('ascii').rstrip() for record in InSDE_NotInComplus})
            print InSDE_query_tup
            if len(InSDE_query_tup) == 1:
                InSDE_query = "LICENSE = '{}'".format(InSDE_query_tup[0])
            else:
                InSDE_query = "LICENSE IN {}".format(InSDE_query_tup)
            alco_license_lyr = arcpy.MakeFeatureLayer_management(alco_license, 'alco_license_lyr')
            arcpy.SelectLayerByAttribute_management(alco_license_lyr, "NEW_SELECTION", InSDE_query)
            #   following logic ensures there is a selection whose quantity equals number of licenses to remove so that
            #   DeleteFeatures doesn't delete entire fc. That's never happened. ever.
            if int(arcpy.GetCount_management(alco_license_lyr)[0]) == (len(InSDE_NotInComplus)):
                arcpy.DeleteFeatures_management(alco_license_lyr)
            else:
                print "count of selected records in alco_license_lyr != len(InSDE_notInComplus) line 156"

            alco_license_tblview = arcpy.MakeTableView_management(Planning_Alcohol_License_fullpath, 'alco_license_tblview')
            arcpy.SelectLayerByAttribute_management(alco_license_tblview, "NEW_SELECTION", InSDE_query)
            if int(arcpy.GetCount_management(alco_license_tblview)[0]) == (len(InSDE_NotInComplus)):
                arcpy.DeleteRows_management(alco_license_tblview)
            else:
                print "count of selected records in grouphomes_tbleview != len(InSDE_NotInComplus) line 164"


            sendMail("Alcohol License App deleted licenses",
                     # 'jssawyer@wpb.org',
                 ['cdglass@wpb.org', 'jssawyer@wpb.org'],
                 "These have been deleted from Planning.SDE.AlcoholLicense_complus feature class and "
                    "Planning.SDE.WPB_GIS_ALCOHOL_LICENSES:",
                 "{}".format(InSDE_query_tup))

            with open(logfile, "a") as log:
                now = datetime.datetime.now().strftime("%m-%d-%Y")
                log.write("\n------------------------------------------\n\n")
                log.write(now)
                log.write('\n')
                log.write('This license has been deleted:')
                log.write(str(InSDE_query_tup))
                log.write("\n")

    except Exception as E:
        sendMail("Alcohol License script failure report",
                 "jssawyer@wpb.org",
                 "An error occurred. Here are the Type, arguments, and log of the errors",
                 "\n{0}\n{1}\n{2}".format(type(E).__name__ ,E.args,traceback.format_exc()))

    finally:
        del_list = (alco_licence_poly, alco_license_points)
        for fc in del_list:
            arcpy.Delete_management(fc)


def sendMail(subject_param, sendto_param, body_text_param, report_param):
    today = datetime.datetime.now().strftime("%m-%d-%Y")
    subject = "{} {}".format(subject_param, today)
    sender = '****'
    sender_pw = '****'
    server = 'smtp.gmail.com'
    body_text = "From: {0}\nTo: {1}\nSubject: {2}\n" \
                "\n{3}\n{4}" \
        .format(sender, sendto_param, subject, body_text_param, report_param)

    gmail = smtplib.SMTP(server, 587)
    gmail.starttls()
    gmail.login(sender, sender_pw)
    gmail.sendmail(sender, sendto_param, body_text)
    gmail.quit()


main()
