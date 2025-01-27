using OptimaSecureUpsellPremiumValidation.Data;
using OptimaSecureUpsellPremiumValidation.Models.Domain;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using Dapper;
using Oracle.ManagedDataAccess.Client;
using System.Configuration;
using Microsoft.Extensions.Logging;
using HERG_HERGPremiumValidationSchedularAPI_Services.Models.Domain;
using DocumentFormat.OpenXml.InkML;
using Serilog;
using System.Collections;
using System.Data.Common;
using DocumentFormat.OpenXml.VariantTypes;

namespace OptimaSecureUpsellPremiumValidation.BussinessLogic
{
    public class OptimaSecure
    {
        private readonly HDFCDbContext dbContext;
        private readonly ILogger<OptimaSecure> _logger;

        // Constructor injection of ILogger
        public OptimaSecure(HDFCDbContext hDFCDbContext, ILogger<OptimaSecure> logger)
        {
            //this.dbContext = hDFCDbContext;
            _logger = logger;
        }
        public async Task<List<OptimaSecureRNE>> GetGCDataAsync(string policyNo)
        {
            string? connectionString = ConfigurationManager.ConnectionStrings["PostgresDb"].ConnectionString;

            // SQL Query for the join
            string sqlQuery = @"
        SELECT 
         os.prod_code,
                os.prod_name,
               os.policy_number,
                os.policy_start_date,
                os.policy_expiry_date,
              os.policy_period,
               os.tier_type,
                os.policyplan,
               os.policy_type,
                os.txt_family,
                os.num_tot_premium,
                os.num_net_premium,
                os.num_service_tax,

                   osidst.loading_per_insured1,
                    osidst.loading_per_insured2,
                   osidst.loading_per_insured3,
                   osidst.loading_per_insured4,
                  osidst.loading_per_insured5,
                    osidst.loading_per_insured6,
                   osidst.loading_per_insured7,
                    osidst.loading_per_insured8,
                   osidst.loading_per_insured9,
                  osidst.loading_per_insured10,
                    osidst.loading_per_insured11,
                    osidst.loading_per_insured12,

                   os.txt_insuredname1,
                   os.txt_insuredname2,
                  os.txt_insuredname3,
                    os.txt_insuredname4,
                    os.txt_insuredname5,
                    os.txt_insuredname6,
                   os.txt_insuredname7,
                    os.txt_insuredname8,
                   os.txt_insuredname9,
                   os.txt_insuredname10,
                   os.txt_insuredname11,
                   os.txt_insuredname12,

                   os.txt_insured_relation1,
                    os.txt_insured_relation2,
                 os.txt_insured_relation3,
                  os.txt_insured_relation4,
                   os.txt_insured_relation5,
                  os.txt_insured_relation6,
                  os.txt_insured_relation7,
                  os.txt_insured_relation8,
               os.txt_insured_relation9,
                  os.txt_insured_relation10,
                   os.txt_insured_relation11,
                   os.txt_insured_relation12,

                    os.txt_insured_age1,
                    os.txt_insured_age2,
                    os.txt_insured_age3,
                   os.txt_insured_age4,
                    os.txt_insured_age5,
                   os.txt_insured_age6,
                    os.txt_insured_age7,
                  os.txt_insured_age8,
                   os.txt_insured_age9,
                   os.txt_insured_age10,
                    os.txt_insured_age11,
                    os.txt_insured_age12,

                   os.pollddesc1,
                    os.pollddesc2,
                    os.pollddesc3,
                   os.pollddesc4,
                    os.pollddesc5,

                    os.upselltype1,
                   os.upselltype2,
                    os.upselltype3,
                    os.upselltype4,
                    os.upselltype5,

                    os.upsellvalue1,
                    os.upsellvalue2,
                    os.upsellvalue3,
                    os.upsellvalue4,
                   os.upsellvalue5,

                  os.upsellpremium1,
                    os.upsellpremium2,
                    os.upsellpremium3,
                    os.upsellpremium4,
                   os.upsellpremium5,

                   os.sum_insured1,
                    os.sum_insured2,
                    os.sum_insured3,
                   os.sum_insured4,
                    os.sum_insured5,
                    os.sum_insured6,
                    os.sum_insured7,
                    os.sum_insured8,
                    os.sum_insured9,
                    os.sum_insured10,
                    os.sum_insured11,
                    os.sum_insured12,

                   os.insured_cb1,
                    os.insured_cb2,
                   os.insured_cb3,
                   os.insured_cb4,
                   os.insured_cb5,
                    os.insured_cb6,
                    os.insured_cb7,
                   os.insured_cb8,
                   os.insured_cb9,
                    os.insured_cb10,
                    os.insured_cb11,
                    os.insured_cb12,

                    os.covername11,
                    os.covername12,
                   os.covername13,
                   os.covername14,
                   os.covername15,
                    os.covername16,
                    os.covername17,
                    os.covername18,
                    os.covername19,
                    os.covername21,
                    os.covername22,
                    os.covername23,
                   os.covername24,
                   os.covername25,
                   os.covername26,
                    os.covername27,
                  os.covername28,
                   os.covername29,
                    os.covername31,
                   os.covername32,
                    os.covername33,
                os.covername34,
                    os.covername35,
                    os.covername36,
                  os.covername37,
                   os.covername38,
                os.covername39,
                   os.covername41,
                 os.covername42,
                  os.covername43,
                  os.covername44,
               os.covername45,
                  os.covername46,
                  os.covername47,
                   os.covername48,
                    os.covername49,
                    os.covername51,
                   os.covername52,
                  os.covername53,
                   os.covername54,
                os.covername55,
                  os.covername56,
                   os.covername57,
                    os.covername58,
                    os.covername59,
                 os.covername61,
                    os.covername62,
                  os.covername63,
                  os.covername64,
                   os.covername65,
                 os.covername66,
                    os.covername67,
                  os.covername68,
                  os.covername69,
                    os.covername71,
                   os.covername72,
                   os.covername73,
                   os.covername74,
                   os.covername75,
                   os.covername76,
                  os.covername77,
                    os.covername78,
               os.covername79,
                   os.covername81,
               os.covername82,
                    os.covername83,
              os.covername84,
                 os.covername85,
                   os.covername86,
                    os.covername87,
                 os.covername88,
                os.covername89,
                os.covername91,
                  os.covername92,
                os.covername93,
                 os.covername94,
                 os.covername95,
                 os.covername96,
                 os.covername97,
                os.covername98,
                os.covername99,
                 os.covername101,
                os.covername102,
                 os.covername103,
                  os.covername104,
                  os.covername105,
                  os.covername106,
                  os.covername107,
                 os.covername108,
                  os.covername109,
                  os.covername110,
                  os.covername210,
                os.covername310,
                 os.covername410,
                  os.covername510,
                  os.covername610,
                    os.covername710,
                   os.covername810,
                   os.covername910,
                   os.covername1010,

                   os.coversi11,
                 os.coversi12,
                    os.coversi13,
               os.coversi14,
                  os.coversi15,
                  os.coversi16,
                  os.coversi17,
                  os.coversi18,
                 os.coversi19,
                  os.coversi21,
                os.coversi22,
                  os.coversi23,
                  os.coversi24,
                 os.coversi25,
                  os.coversi26,
                 os.coversi27,
                 os.coversi28,
                  os.coversi29,
                  os.coversi31,
                  os.coversi32,
                  os.coversi33,
                 os.coversi34,
                  os.coversi35,
                  os.coversi36,
                 os.coversi37,
                  os.coversi38,
                  os.coversi39,
                 os.coversi41,
                 os.coversi42,
                  os.coversi43,
                  os.coversi44,
                  os.coversi45,
                 os.coversi46,
                 os.coversi47,
                  os.coversi48,
                 os.coversi49,
                  os.coversi51,
                 os.coversi52,
                 os.coversi53,
                 os.coversi54,
                  os.coversi55,
                 os.coversi56,
                  os.coversi57,
                  os.coversi58,
                 os.coversi59,
                  os.coversi61,
                  os.coversi62,
                 os.coversi63,
                 os.coversi64,
                  os.coversi65,
                os.coversi66,
                  os.coversi67,
                  os.coversi68,
                  os.coversi69,
                  os.coversi71,
                  os.coversi72,
                  os.coversi73,
                  os.coversi74,
                 os.coversi75,
                  os.coversi76,
                  os.coversi77,
                  os.coversi78,
                  os.coversi79,
                  os.coversi81,
                  os.coversi82,
                  os.coversi83,
                 os.coversi84,
                 os.coversi85,
                 os.coversi86,
                 os.coversi87,
                  os.coversi88,
                 os.coversi89,
                 os.coversi91,
                os.coversi92,
                  os.coversi93,
                os.coversi94,
                 os.coversi95,
                 os.coversi96,
                os.coversi97,
                 os.coversi98,
                  os.coversi99,
                os.coversi101,
                  os.coversi102,
                  os.coversi103,
                  os.coversi104,
                 os.coversi105,
                  os.coversi106,
                 os.coversi107,
                 os.coversi108,
                 os.coversi109,
                os.coversi210,
                  os.coversi310,
                os.coversi410,
                 os.coversi510,
                os.coversi610,
                  os.coversi810,
                os.coversi910,
               os.coversi1010,

                   os.coverprem11,
                  os.coverprem12,
                    os.coverprem13,
                    os.coverprem14,
                    os.coverprem15,
                    os.coverprem16,
     os.coverprem17,
                 os.coverprem18,
                os.coverprem19,
                  os.coverprem21,
               os.coverprem22,
              os.coverprem23,
                os.coverprem24,
                 os.coverprem25,
               os.coverprem26,
                os.coverprem27,
               os.coverprem28,
               os.coverprem29,
                  os.coverprem31,
                  os.coverprem32,
                 os.coverprem33,
                os.coverprem34,
                 os.coverprem35,
                 os.coverprem36,
                 os.coverprem37,
                  os.coverprem38,
                os.coverprem39,
                 os.coverprem41,
                  os.coverprem42,
                os.coverprem43,
                 os.coverprem44,
                os.coverprem46,
                 os.coverprem47,
                  os.coverprem48,
                  os.coverprem49,
                 os.coverprem51,
               os.coverprem52,
                os.coverprem53,
               os.coverprem54,
                 os.coverprem55,
                  os.coverprem56,
                  os.coverprem57,
                 os.coverprem58,
                  os.coverprem59,
                 os.coverprem61,
                  os.coverprem62,
                 os.coverprem63,
                 os.coverprem64,
                os.coverprem65,
                 os.coverprem66,
                  os.coverprem67,
             os.coverprem68,
              os.coverprem69,
                  os.coverprem71,
               os.coverprem72,
                  os.coverprem73,
                 os.coverprem74,
                 
                  os.coverprem75,
                    os.coverprem76,
                    os.coverprem77,
                  os.coverprem78,
                    os.coverprem79,
                    os.coverprem81,
                   os.coverprem82,
                   os.coverprem83,
                 os.coverprem84,
                  os.coverprem85,
                    os.coverprem86,
                    os.coverprem87,
                    os.coverprem88,
                   os.coverprem89,
                    os.coverprem91,
                   os.coverprem92,
                   os.coverprem93,
                    os.coverprem94,
                    os.coverprem95,
                    os.coverprem96,
                    os.coverprem97,
                    os.coverprem98,
                    os.coverprem99,
                   os.coverprem101,
                    os.coverprem102,
                    os.coverprem103,
                    os.coverprem104,
                   os.coverprem105,
                   os.coverprem106,
                    os.coverprem107,
                   os.coverprem108,
                    os.coverprem109,
                   os.coverprem210,
                  os.coverprem310,
                 os.coverprem410,
                    os.coverprem510,
                  os.coverprem610,
                  os.coverprem810,
                os.coverprem910,
                  os.coverprem1010,

                    os.coverloadingrate11,
                    os.coverloadingrate12,
                  os.coverloadingrate13,
                   os.coverloadingrate14,
                   os.coverloadingrate15,
                    os.coverloadingrate16,
                    os.coverloadingrate17,
                    os.coverloadingrate18,
                   os.coverloadingrate19,
                    os.coverloadingrate21,
                 os.coverloadingrate22,
                   os.coverloadingrate23,
                  os.coverloadingrate24,
                    os.coverloadingrate25,
                    os.coverloadingrate26,
                   os.coverloadingrate27,
                    os.coverloadingrate28,
                    os.coverloadingrate29,
                    os.coverloadingrate31,
                 os.coverloadingrate32,
                    os.coverloadingrate33,
                    os.coverloadingrate34,
                 os.coverloadingrate35,
                   os.coverloadingrate36,
                  os.coverloadingrate37,
                    os.coverloadingrate38,
                    os.coverloadingrate39,
                os.coverloadingrate41,
                   os.coverloadingrate42,
                    os.coverloadingrate43,
                   os.coverloadingrate44,
                    os.coverloadingrate45,
                    os.coverloadingrate46,
                    os.coverloadingrate47,
                  os.coverloadingrate48,
                   os.coverloadingrate49,
                 os.coverloadingrate51,
                   os.coverloadingrate52,
                  os.coverloadingrate53,
                   os.coverloadingrate54,
                  os.coverloadingrate55,
                    os.coverloadingrate56,
                    os.coverloadingrate57,
                  os.coverloadingrate58,
                    os.coverloadingrate59,
                   os.coverloadingrate61,
                 os.coverloadingrate62,
                  os.coverloadingrate63,
                    os.coverloadingrate64,
                    os.coverloadingrate65,
                    os.coverloadingrate66,
                    os.coverloadingrate67,
                os.coverloadingrate68,
                   os.coverloadingrate69,
                 os.coverloadingrate71,
                  os.coverloadingrate72,
                   os.coverloadingrate73,
                    os.coverloadingrate74,
                  os.coverloadingrate75,
                os.coverloadingrate76,
                 os.coverloadingrate77,
               os.coverloadingrate78,
                os.coverloadingrate79,
                  os.coverloadingrate81,
                   os.coverloadingrate82,
                  os.coverloadingrate83,
                    os.coverloadingrate84,
                 os.coverloadingrate85,
                   os.coverloadingrate86,
               os.coverloadingrate87,
                  os.coverloadingrate88,
                 os.coverloadingrate89,
                    os.coverloadingrate91,
                    os.coverloadingrate92,
                os.coverloadingrate93,
                  os.coverloadingrate94,
                 os.coverloadingrate95,
                   os.coverloadingrate96,
                  os.coverloadingrate97,
                   os.coverloadingrate98,
                   os.coverloadingrate99,
                   os.coverloadingrate101,
             os.coverloadingrate102,
                   os.coverloadingrate103,
                 os.coverloadingrate104,
                   os.coverloadingrate105,
                 os.coverloadingrate106,
                 os.coverloadingrate107,
                   os.coverloadingrate108,
                os.coverloadingrate109,
                 os.coverloadingrate210,
                   os.coverloadingrate310,
                os.coverloadingrate410,
                    os.coverloadingrate510,
                   os.coverloadingrate610,
                os.coverloadingrate810,
                    os.coverloadingrate910,
                    os.coverloadingrate1010
        FROM 
            ins.rne_healthtab os
        INNER JOIN 
            ins.idst_renewal_data_rgs osidst ON os.policy_number = osidst.certificate_no
        WHERE 
            os.policy_number = @PolicyNo";
            try
            {
                // Using Dapper to execute the query
                using (var connection = new NpgsqlConnection(connectionString))
                {
                    //

                    // Execute the query asynchronously and map the results to OptimaSecureRNE
                    var osRNEData = await connection.QueryAsync<OptimaSecureRNE>(sqlQuery, new { PolicyNo = policyNo }).ConfigureAwait(false);
                    // Log the data (you can add more logging or details as needed)
                    //Log.Information("gc");

                    // Return the list of results
                    return osRNEData.ToList();
                }
            }
            catch (Exception ex)
            {
                // Log the error (you can replace this with your preferred logging mechanism)
                Log.Error(ex, "An error occurred while fetching GC data for policy: {PolicyNo}", policyNo);
                //throw; // Re-throw the exception to allow higher-level handlers to catch it
                return new List<OptimaSecureRNE>();
            }
        }
        async Task<verifiedpremiumvalues> GetCrosscheckValue(string policyNo, List<OptimaSecureRNE> orRNEData)
        {
            string? connectionString = ConfigurationManager.ConnectionStrings["PostgresDb"].ConnectionString;
            using (IDbConnection dbConnection = new NpgsqlConnection(connectionString))
            {
                dbConnection.Open();
                // Check if the record exists by selecting only required columns
                    var record = dbConnection.QueryFirstOrDefault<premium_validation>(
                        "SELECT certificate_no,verified_prem,verified_gst,verified_total_prem FROM ins.premium_validation WHERE certificate_no = @CertificateNo",
                        new { CertificateNo = policyNo.ToString() });

                if (record != null && record.rn_generation_status == null)
                {
                    decimal? crosscheck1 = (orRNEData.FirstOrDefault()?.num_tot_premium ?? 0) - (record.verified_total_prem ?? 0);
                    return new verifiedpremiumvalues
                    {
                        verified_gst = record.verified_gst ?? 0, 
                        verified_total_premium = record.verified_total_prem ?? 0,
                        verified_net_premium = record.verified_prem ?? 0,
                        crosscheck = crosscheck1
                    };
                
                }
                else
                {
                    return new verifiedpremiumvalues
                    {
                        verified_gst = 0,  // Default values
                        verified_total_premium = 0,
                        verified_net_premium = 0,
                        crosscheck = null
                    };
                }

            }
        }
       
        public async Task<OptimaSecurePremiumValidationUpsell> GetOptimaSecureValidation(string policyNo, Dictionary<string, Hashtable> baseRateHashTable, Dictionary<string, Hashtable> relations, Dictionary<string, Hashtable> cirates, Dictionary<string, Hashtable> deductableDiscount, Dictionary<string, Hashtable> hdcproportionsplit, Dictionary<string, Hashtable> hdcrates)
        {
            List <OptimaSecureRNE> osRNEData;         
            IEnumerable<OptimaSecureRNE> osRNEDataUpSell = Enumerable.Empty<OptimaSecureRNE>();
            osRNEData = await GetGCDataAsync(policyNo);
            if (osRNEData != null && osRNEData.Any())
            {
                foreach (var row in osRNEData)
                {
                    if (row.upselltype1 == "SI_UPSELL" || row.upselltype2 == "SI_UPSELL" || row.upselltype3 == "SI_UPSELL" || row.upselltype4 == "SI_UPSELL" || row.upselltype5 == "SI_UPSELL" || row.upselltype1 == "UPSELLBASESI_1" || row.upselltype2 == "UPSELLBASESI_1" || row.upselltype3 == "UPSELLBASESI_1" || row.upselltype4 == "UPSELLBASESI_1" || row.upselltype5 == "UPSELLBASESI_1")
                    {
                        osRNEDataUpSell = await CalculateOptimaSecurePremiumqUpsell
                            (osRNEData,policyNo, baseRateHashTable, relations, cirates, deductableDiscount, hdcproportionsplit, hdcrates);
                        break;
                    }
                }
            }
            //var premiumvalues = await GetCrosscheckValue(policyNo, osRNEData);

            //Combine the result sets and send it in the response.
            //Note: To compute with base SI Premiums for Insureds.
            OptimaSecurePremiumValidationUpsell objOptimaSecurePremiumValidationUpSell = new OptimaSecurePremiumValidationUpsell
            {
                policy_number = osRNEData.FirstOrDefault()?.policy_number,
                reference_num = osRNEData.FirstOrDefault()?.reference_num,
                prod_code = osRNEData.FirstOrDefault()?.prod_code,
                prod_name = osRNEData.FirstOrDefault()?.prod_name,
                batchid = osRNEData.FirstOrDefault()?.batchid,
                customer_id = osRNEData.FirstOrDefault()?.customer_id,
                customername = osRNEData.FirstOrDefault()?.customername,
                policy_start_date = osRNEData.FirstOrDefault()?.policy_start_date,
                policy_expiry_date = osRNEData.FirstOrDefault()?.policy_expiry_date,
                txt_salutation = osRNEData.FirstOrDefault()?.txt_salutation,
                location_code = osRNEData.FirstOrDefault()?.location_code,
                txt_apartment = osRNEData.FirstOrDefault()?.txt_apartment,
                txt_street = osRNEData.FirstOrDefault()?.txt_street,
                txt_areavillage = osRNEData.FirstOrDefault()?.txt_areavillage,
                txt_citydistrict = osRNEData.FirstOrDefault()?.txt_citydistrict,
                txt_state = osRNEData.FirstOrDefault()?.txt_state,
                state_code = osRNEData.FirstOrDefault()?.state_code,
                state_regis = osRNEData.FirstOrDefault()?.state_regis,
                txt_pincode = osRNEData.FirstOrDefault()?.txt_pincode,
                txt_nationality = osRNEData.FirstOrDefault()?.txt_nationality,
                split_flag = osRNEData.FirstOrDefault()?.split_flag,
                txt_family = osRNEData.FirstOrDefault()?.txt_family,
                policyplan = osRNEData.FirstOrDefault()?.policyplan,
                policy_type = osRNEData.FirstOrDefault()?.policy_type,
                policy_period = osRNEData.FirstOrDefault()?.policy_period,
                verticalname = osRNEData.FirstOrDefault()?.verticalname,
                vertical_name = osRNEData.FirstOrDefault()?.vertical_name,
                no_of_members = osRNEData.FirstOrDefault()?.no_of_members,
                eldest_member = osRNEData.FirstOrDefault()?.eldest_member,

                optima_secure_gst = osRNEData.FirstOrDefault()?.optima_secure_gst.HasValue == true
    ? (decimal?)Math.Round(osRNEData.FirstOrDefault().optima_secure_gst.Value, 2)
    : (decimal?)null,

                upselltype1 = osRNEDataUpSell.FirstOrDefault()?.upselltype1,
                upselltype2 = osRNEDataUpSell.FirstOrDefault()?.upselltype2,
                upselltype3 = osRNEDataUpSell.FirstOrDefault()?.upselltype3,
                upselltype4 = osRNEDataUpSell.FirstOrDefault()?.upselltype4,
                upselltype5 = osRNEDataUpSell.FirstOrDefault()?.upselltype5,

                upsellvalue1 = osRNEDataUpSell.FirstOrDefault()?.upsellvalue1,
                upsellvalue2 = osRNEDataUpSell.FirstOrDefault()?.upsellvalue2,
                upsellvalue3 = osRNEDataUpSell.FirstOrDefault()?.upsellvalue3,
                upsellvalue4 = osRNEDataUpSell.FirstOrDefault()?.upsellvalue4,
                upsellvalue5 = osRNEDataUpSell.FirstOrDefault()?.upsellvalue5,

                upsellpremium1 = osRNEDataUpSell.FirstOrDefault()?.upsellpremium1,
                upsellpremium2 = osRNEDataUpSell.FirstOrDefault()?.upsellpremium2,
                upsellpremium3 = osRNEDataUpSell.FirstOrDefault()?.upsellpremium3,
                upsellpremium4 = osRNEDataUpSell.FirstOrDefault()?.upsellpremium4,
                upsellpremium5 = osRNEDataUpSell.FirstOrDefault()?.upsellpremium5,

                insured_loadingper1 = osRNEData.FirstOrDefault()?.insured_loadingper1,
                insured_loadingper2 = osRNEData.FirstOrDefault()?.insured_loadingper2,
                insured_loadingper3 = osRNEData.FirstOrDefault()?.insured_loadingper3,
                insured_loadingper4 = osRNEData.FirstOrDefault()?.insured_loadingper4,
                insured_loadingper5 = osRNEData.FirstOrDefault()?.insured_loadingper5,
                insured_loadingper6 = osRNEData.FirstOrDefault()?.insured_loadingper6,
                insured_loadingper7 = osRNEData.FirstOrDefault()?.insured_loadingper7,
                insured_loadingper8 = osRNEData.FirstOrDefault()?.insured_loadingper8,
                insured_loadingper9 = osRNEData.FirstOrDefault()?.insured_loadingper9,
                insured_loadingper10 = osRNEData.FirstOrDefault()?.insured_loadingper10,
                insured_loadingper11 = osRNEData.FirstOrDefault()?.insured_loadingper11,
                insured_loadingper12 = osRNEData.FirstOrDefault()?.insured_loadingper12,

                txt_insuredname1 = osRNEData.FirstOrDefault()?.txt_insuredname1,
                txt_insuredname2 = osRNEData.FirstOrDefault()?.txt_insuredname2,
                txt_insuredname3 = osRNEData.FirstOrDefault()?.txt_insuredname3,
                txt_insuredname4 = osRNEData.FirstOrDefault()?.txt_insuredname4,
                txt_insuredname5 = osRNEData.FirstOrDefault()?.txt_insuredname5,
                txt_insuredname6 = osRNEData.FirstOrDefault()?.txt_insuredname6,
                txt_insuredname7 = osRNEData.FirstOrDefault()?.txt_insuredname7,
                txt_insuredname8 = osRNEData.FirstOrDefault()?.txt_insuredname8,
                txt_insuredname9 = osRNEData.FirstOrDefault()?.txt_insuredname9,
                txt_insuredname10 = osRNEData.FirstOrDefault()?.txt_insuredname10,
                txt_insuredname11 = osRNEData.FirstOrDefault()?.txt_insuredname11,
                txt_insuredname12 = osRNEData.FirstOrDefault()?.txt_insuredname12,

                txt_insured_entrydate1 = osRNEData.FirstOrDefault()?.txt_insured_entrydate1,
                txt_insured_entrydate2 = osRNEData.FirstOrDefault()?.txt_insured_entrydate2,
                txt_insured_entrydate3 = osRNEData.FirstOrDefault()?.txt_insured_entrydate3,
                txt_insured_entrydate4 = osRNEData.FirstOrDefault()?.txt_insured_entrydate4,
                txt_insured_entrydate5 = osRNEData.FirstOrDefault()?.txt_insured_entrydate5,
                txt_insured_entrydate6 = osRNEData.FirstOrDefault()?.txt_insured_entrydate6,
                txt_insured_entrydate7 = osRNEData.FirstOrDefault()?.txt_insured_entrydate7,
                txt_insured_entrydate8 = osRNEData.FirstOrDefault()?.txt_insured_entrydate8,
                txt_insured_entrydate9 = osRNEData.FirstOrDefault()?.txt_insured_entrydate9,
                txt_insured_entrydate10 = osRNEData.FirstOrDefault()?.txt_insured_entrydate10,
                txt_insured_entrydate11 = osRNEData.FirstOrDefault()?.txt_insured_entrydate11,
                txt_insured_entrydate12 = osRNEData.FirstOrDefault()?.txt_insured_entrydate12,

                member_id1 = osRNEData.FirstOrDefault()?.member_id1,
                member_id2 = osRNEData.FirstOrDefault()?.member_id2,
                member_id3 = osRNEData.FirstOrDefault()?.member_id3,
                member_id4 = osRNEData.FirstOrDefault()?.member_id4,
                member_id5 = osRNEData.FirstOrDefault()?.member_id5,
                member_id6 = osRNEData.FirstOrDefault()?.member_id6,
                member_id7 = osRNEData.FirstOrDefault()?.member_id7,
                member_id8 = osRNEData.FirstOrDefault()?.member_id8,
                member_id9 = osRNEData.FirstOrDefault()?.member_id9,
                member_id10 = osRNEData.FirstOrDefault()?.member_id10,
                member_id11 = osRNEData.FirstOrDefault()?.member_id11,
                member_id12 = osRNEData.FirstOrDefault()?.member_id12,

                insured_loadingamt1 = osRNEData.FirstOrDefault()?.insured_loadingamt1,
                insured_loadingamt2 = osRNEData.FirstOrDefault()?.insured_loadingamt2,
                insured_loadingamt3 = osRNEData.FirstOrDefault()?.insured_loadingamt3,
                insured_loadingamt4 = osRNEData.FirstOrDefault()?.insured_loadingamt4,
                insured_loadingamt5 = osRNEData.FirstOrDefault()?.insured_loadingamt5,
                insured_loadingamt6 = osRNEData.FirstOrDefault()?.insured_loadingamt6,
                insured_loadingamt7 = osRNEData.FirstOrDefault()?.insured_loadingamt7,
                insured_loadingamt8 = osRNEData.FirstOrDefault()?.insured_loadingamt8,
                insured_loadingamt9 = osRNEData.FirstOrDefault()?.insured_loadingamt9,
                insured_loadingamt10 = osRNEData.FirstOrDefault()?.insured_loadingamt10,
                insured_loadingamt11 = osRNEData.FirstOrDefault()?.insured_loadingamt11,
                insured_loadingamt12 = osRNEData.FirstOrDefault()?.insured_loadingamt12,

                txt_insured_dob1 = osRNEData.FirstOrDefault()?.txt_insured_dob1,
                txt_insured_dob2 = osRNEData.FirstOrDefault()?.txt_insured_dob2,
                txt_insured_dob3 = osRNEData.FirstOrDefault()?.txt_insured_dob3,
                txt_insured_dob4 = osRNEData.FirstOrDefault()?.txt_insured_dob4,
                txt_insured_dob5 = osRNEData.FirstOrDefault()?.txt_insured_dob5,
                txt_insured_dob6 = osRNEData.FirstOrDefault()?.txt_insured_dob6,
                txt_insured_dob7 = osRNEData.FirstOrDefault()?.txt_insured_dob7,
                txt_insured_dob8 = osRNEData.FirstOrDefault()?.txt_insured_dob8,
                txt_insured_dob9 = osRNEData.FirstOrDefault()?.txt_insured_dob9,
                txt_insured_dob10 = osRNEData.FirstOrDefault()?.txt_insured_dob10,
                txt_insured_dob11 = osRNEData.FirstOrDefault()?.txt_insured_dob11,
                txt_insured_dob12 = osRNEData.FirstOrDefault()?.txt_insured_dob12,

                txt_insured_age1 = osRNEData.FirstOrDefault()?.txt_insured_age1,
                txt_insured_age2 = osRNEData.FirstOrDefault()?.txt_insured_age2,
                txt_insured_age3 = osRNEData.FirstOrDefault()?.txt_insured_age3,
                txt_insured_age4 = osRNEData.FirstOrDefault()?.txt_insured_age4,
                txt_insured_age5 = osRNEData.FirstOrDefault()?.txt_insured_age5,
                txt_insured_age6 = osRNEData.FirstOrDefault()?.txt_insured_age6,
                txt_insured_age7 = osRNEData.FirstOrDefault()?.txt_insured_age7,
                txt_insured_age8 = osRNEData.FirstOrDefault()?.txt_insured_age8,
                txt_insured_age9 = osRNEData.FirstOrDefault()?.txt_insured_age9,
                txt_insured_age10 = osRNEData.FirstOrDefault()?.txt_insured_age10,
                txt_insured_age11 = osRNEData.FirstOrDefault()?.txt_insured_age11,
                txt_insured_age12 = osRNEData.FirstOrDefault()?.txt_insured_age12,

                txt_insured_relation1 = osRNEData.FirstOrDefault()?.txt_insured_relation1,
                txt_insured_relation2 = osRNEData.FirstOrDefault()?.txt_insured_relation2,
                txt_insured_relation3 = osRNEData.FirstOrDefault()?.txt_insured_relation3,
                txt_insured_relation4 = osRNEData.FirstOrDefault()?.txt_insured_relation4,
                txt_insured_relation5 = osRNEData.FirstOrDefault()?.txt_insured_relation5,
                txt_insured_relation6 = osRNEData.FirstOrDefault()?.txt_insured_relation6,
                txt_insured_relation7 = osRNEData.FirstOrDefault()?.txt_insured_relation7,
                txt_insured_relation8 = osRNEData.FirstOrDefault()?.txt_insured_relation8,
                txt_insured_relation9 = osRNEData.FirstOrDefault()?.txt_insured_relation9,
                txt_insured_relation10 = osRNEData.FirstOrDefault()?.txt_insured_relation10,
                txt_insured_relation11 = osRNEData.FirstOrDefault()?.txt_insured_relation11,
                txt_insured_relation12 = osRNEData.FirstOrDefault()?.txt_insured_relation12,

                insured_relation_tag_1 = osRNEData.FirstOrDefault()?.insured_relation_tag_1,
                insured_relation_tag_2 = osRNEData.FirstOrDefault()?.insured_relation_tag_2,
                insured_relation_tag_3 = osRNEData.FirstOrDefault()?.insured_relation_tag_3,
                insured_relation_tag_4 = osRNEData.FirstOrDefault()?.insured_relation_tag_4,
                insured_relation_tag_5 = osRNEData.FirstOrDefault()?.insured_relation_tag_5,
                insured_relation_tag_6 = osRNEData.FirstOrDefault()?.insured_relation_tag_6,
                insured_relation_tag_7 = osRNEData.FirstOrDefault()?.insured_relation_tag_7,
                insured_relation_tag_8 = osRNEData.FirstOrDefault()?.insured_relation_tag_8,
                insured_relation_tag_9 = osRNEData.FirstOrDefault()?.insured_relation_tag_9,
                insured_relation_tag_10 = osRNEData.FirstOrDefault()?.insured_relation_tag_10,
                insured_relation_tag_11 = osRNEData.FirstOrDefault()?.insured_relation_tag_11,
                insured_relation_tag_12 = osRNEData.FirstOrDefault()?.insured_relation_tag_12,

                pre_existing_disease1 = osRNEData.FirstOrDefault()?.pre_existing_disease1,
                pre_existing_disease2 = osRNEData.FirstOrDefault()?.pre_existing_disease2,
                pre_existing_disease3 = osRNEData.FirstOrDefault()?.pre_existing_disease3,
                pre_existing_disease4 = osRNEData.FirstOrDefault()?.pre_existing_disease4,
                pre_existing_disease5 = osRNEData.FirstOrDefault()?.pre_existing_disease5,
                pre_existing_disease6 = osRNEData.FirstOrDefault()?.pre_existing_disease6,
                pre_existing_disease7 = osRNEData.FirstOrDefault()?.pre_existing_disease7,
                pre_existing_disease8 = osRNEData.FirstOrDefault()?.pre_existing_disease8,
                pre_existing_disease9 = osRNEData.FirstOrDefault()?.pre_existing_disease9,
                pre_existing_disease10 = osRNEData.FirstOrDefault()?.pre_existing_disease10,
                pre_existing_disease11 = osRNEData.FirstOrDefault()?.pre_existing_disease11,
                pre_existing_disease12 = osRNEData.FirstOrDefault()?.pre_existing_disease12,

                insured_cb1 = osRNEData.FirstOrDefault()?.insured_cb1,
                insured_cb2 = osRNEData.FirstOrDefault()?.insured_cb2,
                insured_cb3 = osRNEData.FirstOrDefault()?.insured_cb3,
                insured_cb4 = osRNEData.FirstOrDefault()?.insured_cb4,
                insured_cb5 = osRNEData.FirstOrDefault()?.insured_cb5,
                insured_cb6 = osRNEData.FirstOrDefault()?.insured_cb6,
                insured_cb7 = osRNEData.FirstOrDefault()?.insured_cb7,
                insured_cb8 = osRNEData.FirstOrDefault()?.insured_cb8,
                insured_cb9 = osRNEData.FirstOrDefault()?.insured_cb9,
                insured_cb10 = osRNEData.FirstOrDefault()?.insured_cb10,
                insured_cb11 = osRNEData.FirstOrDefault()?.insured_cb11,
                insured_cb12 = osRNEData.FirstOrDefault()?.insured_cb12,

                sum_insured1 = osRNEData.FirstOrDefault()?.sum_insured1,
                sum_insured2 = osRNEData.FirstOrDefault()?.sum_insured2,
                sum_insured3 = osRNEData.FirstOrDefault()?.sum_insured3,
                sum_insured4 = osRNEData.FirstOrDefault()?.sum_insured4,
                sum_insured5 = osRNEData.FirstOrDefault()?.sum_insured5,
                sum_insured6 = osRNEData.FirstOrDefault()?.sum_insured6,
                sum_insured7 = osRNEData.FirstOrDefault()?.sum_insured7,
                sum_insured8 = osRNEData.FirstOrDefault()?.sum_insured8,
                sum_insured9 = osRNEData.FirstOrDefault()?.sum_insured9,
                sum_insured10 = osRNEData.FirstOrDefault()?.sum_insured10,
                sum_insured11 = osRNEData.FirstOrDefault()?.sum_insured11,
                sum_insured12 = osRNEData.FirstOrDefault()?.sum_insured12,

                insured_deductable1 = osRNEData.FirstOrDefault()?.insured_deductable1,
                insured_deductable2 = osRNEData.FirstOrDefault()?.insured_deductable2,
                insured_deductable3 = osRNEData.FirstOrDefault()?.insured_deductable3,
                insured_deductable4 = osRNEData.FirstOrDefault()?.insured_deductable4,
                insured_deductable5 = osRNEData.FirstOrDefault()?.insured_deductable5,
                insured_deductable6 = osRNEData.FirstOrDefault()?.insured_deductable6,
                insured_deductable7 = osRNEData.FirstOrDefault()?.insured_deductable7,
                insured_deductable8 = osRNEData.FirstOrDefault()?.insured_deductable8,
                insured_deductable9 = osRNEData.FirstOrDefault()?.insured_deductable9,
                insured_deductable10 = osRNEData.FirstOrDefault()?.insured_deductable10,
                insured_deductable11 = osRNEData.FirstOrDefault()?.insured_deductable11,
                insured_deductable12 = osRNEData.FirstOrDefault()?.insured_deductable12,

                wellness_discount1 = osRNEData.FirstOrDefault()?.wellness_discount1,
                wellness_discount2 = osRNEData.FirstOrDefault()?.wellness_discount2,
                wellness_discount3 = osRNEData.FirstOrDefault()?.wellness_discount3,
                wellness_discount4 = osRNEData.FirstOrDefault()?.wellness_discount4,
                wellness_discount5 = osRNEData.FirstOrDefault()?.wellness_discount5,
                wellness_discount6 = osRNEData.FirstOrDefault()?.wellness_discount6,
                wellness_discount7 = osRNEData.FirstOrDefault()?.wellness_discount7,
                wellness_discount8 = osRNEData.FirstOrDefault()?.wellness_discount8,
                wellness_discount9 = osRNEData.FirstOrDefault()?.wellness_discount9,
                wellness_discount10 = osRNEData.FirstOrDefault()?.wellness_discount10,
                wellness_discount11 = osRNEData.FirstOrDefault()?.wellness_discount11,
                wellness_discount12 = osRNEData.FirstOrDefault()?.wellness_discount12,

                stayactive1 = osRNEData.FirstOrDefault()?.stayactive1,
                stayactive2 = osRNEData.FirstOrDefault()?.stayactive2,
                stayactive3 = osRNEData.FirstOrDefault()?.stayactive3,
                stayactive4 = osRNEData.FirstOrDefault()?.stayactive4,
                stayactive5 = osRNEData.FirstOrDefault()?.stayactive5,
                stayactive6 = osRNEData.FirstOrDefault()?.stayactive6,
                stayactive7 = osRNEData.FirstOrDefault()?.stayactive7,
                stayactive8 = osRNEData.FirstOrDefault()?.stayactive8,
                stayactive9 = osRNEData.FirstOrDefault()?.stayactive9,
                stayactive10 = osRNEData.FirstOrDefault()?.stayactive10,
                stayactive11 = osRNEData.FirstOrDefault()?.stayactive11,
                stayactive12 = osRNEData.FirstOrDefault()?.stayactive12,

                coverbaseloadingrate1 = osRNEData.FirstOrDefault()?.coverbaseloadingrate1,
                coverbaseloadingrate2 = osRNEData.FirstOrDefault()?.coverbaseloadingrate2,
                coverbaseloadingrate3 = osRNEData.FirstOrDefault()?.coverbaseloadingrate3,
                coverbaseloadingrate4 = osRNEData.FirstOrDefault()?.coverbaseloadingrate4,
                coverbaseloadingrate5 = osRNEData.FirstOrDefault()?.coverbaseloadingrate5,
                coverbaseloadingrate6 = osRNEData.FirstOrDefault()?.coverbaseloadingrate6,
                coverbaseloadingrate7 = osRNEData.FirstOrDefault()?.coverbaseloadingrate7,
                coverbaseloadingrate8 = osRNEData.FirstOrDefault()?.coverbaseloadingrate8,
                coverbaseloadingrate9 = osRNEData.FirstOrDefault()?.coverbaseloadingrate9,
                coverbaseloadingrate10 = osRNEData.FirstOrDefault()?.coverbaseloadingrate10,
                coverbaseloadingrate11 = osRNEData.FirstOrDefault()?.coverbaseloadingrate11,
                coverbaseloadingrate12 = osRNEData.FirstOrDefault()?.coverbaseloadingrate12,

                health_incentive1 = osRNEData.FirstOrDefault()?.health_incentive1,
                health_incentive2 = osRNEData.FirstOrDefault()?.health_incentive2,
                health_incentive3 = osRNEData.FirstOrDefault()?.health_incentive3,
                health_incentive4 = osRNEData.FirstOrDefault()?.health_incentive4,
                health_incentive5 = osRNEData.FirstOrDefault()?.health_incentive5,
                health_incentive6 = osRNEData.FirstOrDefault()?.health_incentive6,
                health_incentive7 = osRNEData.FirstOrDefault()?.health_incentive7,
                health_incentive8 = osRNEData.FirstOrDefault()?.health_incentive8,
                health_incentive9 = osRNEData.FirstOrDefault()?.health_incentive9,
                health_incentive10 = osRNEData.FirstOrDefault()?.health_incentive10,
                health_incentive11 = osRNEData.FirstOrDefault()?.health_incentive11,
                health_incentive12 = osRNEData.FirstOrDefault()?.health_incentive12,

                fitness_discount1 = osRNEData.FirstOrDefault()?.fitness_discount1,
                fitness_discount2 = osRNEData.FirstOrDefault()?.fitness_discount2,
                fitness_discount3 = osRNEData.FirstOrDefault()?.fitness_discount3,
                fitness_discount4 = osRNEData.FirstOrDefault()?.fitness_discount4,
                fitness_discount5 = osRNEData.FirstOrDefault()?.fitness_discount5,
                fitness_discount6 = osRNEData.FirstOrDefault()?.fitness_discount6,
                fitness_discount7 = osRNEData.FirstOrDefault()?.fitness_discount7,
                fitness_discount8 = osRNEData.FirstOrDefault()?.fitness_discount8,
                fitness_discount9 = osRNEData.FirstOrDefault()?.fitness_discount9,
                fitness_discount10 = osRNEData.FirstOrDefault()?.fitness_discount10,
                fitness_discount11 = osRNEData.FirstOrDefault()?.fitness_discount11,
                fitness_discount12 = osRNEData.FirstOrDefault()?.fitness_discount12,

                reservbenefis1 = osRNEData.FirstOrDefault()?.reservbenefis1,
                reservbenefis2 = osRNEData.FirstOrDefault()?.reservbenefis2,
                reservbenefis3 = osRNEData.FirstOrDefault()?.reservbenefis3,
                reservbenefis4 = osRNEData.FirstOrDefault()?.reservbenefis4,
                reservbenefis5 = osRNEData.FirstOrDefault()?.reservbenefis5,
                reservbenefis6 = osRNEData.FirstOrDefault()?.reservbenefis6,
                reservbenefis7 = osRNEData.FirstOrDefault()?.reservbenefis7,
                reservbenefis8 = osRNEData.FirstOrDefault()?.reservbenefis8,
                reservbenefis9 = osRNEData.FirstOrDefault()?.reservbenefis9,
                reservbenefis10 = osRNEData.FirstOrDefault()?.reservbenefis10,
                reservbenefis11 = osRNEData.FirstOrDefault()?.reservbenefis11,
                reservbenefis12 = osRNEData.FirstOrDefault()?.reservbenefis12,

                insured_rb_claimamt1 = osRNEData.FirstOrDefault()?.insured_rb_claimamt1,
                insured_rb_claimamt2 = osRNEData.FirstOrDefault()?.insured_rb_claimamt2,
                insured_rb_claimamt3 = osRNEData.FirstOrDefault()?.insured_rb_claimamt3,
                insured_rb_claimamt4 = osRNEData.FirstOrDefault()?.insured_rb_claimamt4,
                insured_rb_claimamt5 = osRNEData.FirstOrDefault()?.insured_rb_claimamt5,
                insured_rb_claimamt6 = osRNEData.FirstOrDefault()?.insured_rb_claimamt6,
                insured_rb_claimamt7 = osRNEData.FirstOrDefault()?.insured_rb_claimamt7,
                insured_rb_claimamt8 = osRNEData.FirstOrDefault()?.insured_rb_claimamt8,
                insured_rb_claimamt9 = osRNEData.FirstOrDefault()?.insured_rb_claimamt9,
                insured_rb_claimamt10 = osRNEData.FirstOrDefault()?.insured_rb_claimamt10,
                insured_rb_claimamt11 = osRNEData.FirstOrDefault()?.insured_rb_claimamt11,
                insured_rb_claimamt12 = osRNEData.FirstOrDefault()?.insured_rb_claimamt12,

                combi_discount = osRNEData.FirstOrDefault()?.combi_discount,
                employee_discount = osRNEData.FirstOrDefault()?.employee_discount,
                online_discount = osRNEData.FirstOrDefault()?.online_discount,
                loyalty_discount = osRNEData.FirstOrDefault()?.loyalty_discount,
                tenure_discount = osRNEData.FirstOrDefault()?.tenure_discount,
                loading_premium = osRNEData.FirstOrDefault()?.loading_premium,
                family_discount = osRNEData.FirstOrDefault()?.family_discount,
                dedcutable_discount = osRNEData.FirstOrDefault()?.dedcutable_discount,

                base_premium_1 = osRNEData.FirstOrDefault()?.base_premium_1,
                base_premium_2 = osRNEData.FirstOrDefault()?.base_premium_2,
                base_premium_3 = osRNEData.FirstOrDefault()?.base_premium_3,
                base_premium_4 = osRNEData.FirstOrDefault()?.base_premium_4,
                base_premium_5 = osRNEData.FirstOrDefault()?.base_premium_5,
                base_premium_6 = osRNEData.FirstOrDefault()?.base_premium_6,
                base_premium_7 = osRNEData.FirstOrDefault()?.base_premium_7,
                base_premium_8 = osRNEData.FirstOrDefault()?.base_premium_8,
                base_premium_9 = osRNEData.FirstOrDefault()?.base_premium_9,
                base_premium_10 = osRNEData.FirstOrDefault()?.base_premium_10,
                base_premium_11 = osRNEData.FirstOrDefault()?.base_premium_11,
                base_premium_12 = osRNEData.FirstOrDefault()?.base_premium_12,
                base_premium = osRNEData.FirstOrDefault()?.base_premium,
                base_premium_after_deductible = osRNEData.FirstOrDefault()?.base_premium_after_deductible,

                loading_prem1 = osRNEData.FirstOrDefault()?.loading_prem1,
                loading_prem2 = osRNEData.FirstOrDefault()?.loading_prem2,
                loading_prem3 = osRNEData.FirstOrDefault()?.loading_prem3,
                loading_prem4 = osRNEData.FirstOrDefault()?.loading_prem4,
                loading_prem5 = osRNEData.FirstOrDefault()?.loading_prem5,
                loading_prem6 = osRNEData.FirstOrDefault()?.loading_prem6,
                loading_prem7 = osRNEData.FirstOrDefault()?.loading_prem7,
                loading_prem8 = osRNEData.FirstOrDefault()?.loading_prem8,
                loading_prem9 = osRNEData.FirstOrDefault()?.loading_prem9,
                loading_prem10 = osRNEData.FirstOrDefault()?.loading_prem10,
                loading_prem11 = osRNEData.FirstOrDefault()?.loading_prem11,
                loading_prem12 = osRNEData.FirstOrDefault()?.loading_prem12,
                loading_prem_total = osRNEData.FirstOrDefault()?.loading_prem_total,

                cash_benefit_loading_prem_1 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_1,
                cash_benefit_loading_prem_2 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_2,
                cash_benefit_loading_prem_3 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_3,
                cash_benefit_loading_prem_4 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_4,
                cash_benefit_loading_prem_5 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_5,
                cash_benefit_loading_prem_6 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_6,
                cash_benefit_loading_prem_7 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_7,
                cash_benefit_loading_prem_8 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_8,
                cash_benefit_loading_prem_9 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_9,
                cash_benefit_loading_prem_10 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_10,
                cash_benefit_loading_prem_11 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_11,
                cash_benefit_loading_prem_12 = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_12,
                cash_benefit_loading_prem_total = osRNEData.FirstOrDefault()?.cash_benefit_loading_prem_total,

                baseAndLoading = osRNEData.FirstOrDefault()?.baseAndLoading,
                baseAndLoading_LoyaltyDiscount = osRNEData.FirstOrDefault()?.baseAndLoading_LoyaltyDiscount,
                baseAndLoading_EmployeeDiscount = osRNEData.FirstOrDefault()?.baseAndLoading_EmployeeDiscount,
                baseAndLoading_OnlineDiscount = osRNEData.FirstOrDefault()?.baseAndLoading_OnlineDiscount,
                baseAndLoading_CombiDiscount = osRNEData.FirstOrDefault()?.baseAndLoading_CombiDiscount,
                baseAndLoading_CapppedDiscount = osRNEData.FirstOrDefault()?.baseAndLoading_CapppedDiscount,
                baseAndLoading_LongTermDiscount = osRNEData.FirstOrDefault()?.baseAndLoading_LongTermDiscount,
                baseAndLoading_OS_Base_Premium = osRNEData.FirstOrDefault()?.baseAndLoading_OS_Base_Premium,

                baseAndLoading_Unlimited_Restore = osRNEData.FirstOrDefault()?.baseAndLoading_Unlimited_Restore,
                baseAndLoading_Final_Base_Premium = osRNEData.FirstOrDefault()?.baseAndLoading_Final_Base_Premium,

                loading_prem_1 = osRNEData.FirstOrDefault()?.loading_prem_1,
                loading_prem_2 = osRNEData.FirstOrDefault()?.loading_prem_2,
                loading_prem_3 = osRNEData.FirstOrDefault()?.loading_prem_3,
                loading_prem_4 = osRNEData.FirstOrDefault()?.loading_prem_4,
                loading_prem_5 = osRNEData.FirstOrDefault()?.loading_prem_5,
                loading_prem_6 = osRNEData.FirstOrDefault()?.loading_prem_6,
                loading_prem_7 = osRNEData.FirstOrDefault()?.loading_prem_7,
                loading_prem_8 = osRNEData.FirstOrDefault()?.loading_prem_8,
                loading_prem_9 = osRNEData.FirstOrDefault()?.loading_prem_9,
                loading_prem_10 = osRNEData.FirstOrDefault()?.loading_prem_10,
                loading_prem_11 = osRNEData.FirstOrDefault()?.loading_prem_11,
                loading_prem_12 = osRNEData.FirstOrDefault()?.loading_prem_12,
                loading_prem = osRNEData.FirstOrDefault()?.loading_prem,

                hDCBaseAndLoading = osRNEData.FirstOrDefault()?.hDCBaseAndLoading,
                HDC_BaseCoverPremium = osRNEData.FirstOrDefault()?.HDC_BaseCoverPremium,
                HDC_LoyaltyDiscount = osRNEData.FirstOrDefault()?.HDC_LoyaltyDiscount,
                HDC_EmployeeDiscount = osRNEData.FirstOrDefault()?.HDC_EmployeeDiscount,
                HDC_OnlineDiscount = osRNEData.FirstOrDefault()?.HDC_OnlineDiscount,
                HDC_FamilyDiscount = osRNEData.FirstOrDefault()?.HDC_FamilyDiscount,
                HDC_CapppedDiscount = osRNEData.FirstOrDefault()?.HDC_CapppedDiscount,
                HDC_LongTermDiscount = osRNEData.FirstOrDefault()?.HDC_LongTermDiscount,

                CI_BaseAndLoading = osRNEData.FirstOrDefault()?.CI_BaseAndLoading,
                CI_BaseCoverPremium = osRNEData.FirstOrDefault()?.CI_BaseCoverPremium,
                CI_LoyaltyDiscount = osRNEData.FirstOrDefault()?.CI_LoyaltyDiscount,
                CI_EmployeeDiscount = osRNEData.FirstOrDefault()?.CI_EmployeeDiscount,
                CI_OnlineDiscount = osRNEData.FirstOrDefault()?.CI_OnlineDiscount,
                CI_FamilyDiscount = osRNEData.FirstOrDefault()?.CI_FamilyDiscount,
                CI_CapppedDiscount = osRNEData.FirstOrDefault()?.CI_CapppedDiscount,
                CI_LongTermDiscount = osRNEData.FirstOrDefault()?.CI_LongTermDiscount,

                cash_Benefit_A = osRNEData.FirstOrDefault()?.cash_Benefit_A,
                cash_Benefit_C = osRNEData.FirstOrDefault()?.cash_Benefit_C,
                cash_Benefit_Age_Band = osRNEData.FirstOrDefault()?.cash_Benefit_Age_Band,
                cash_Benefit_SI = osRNEData.FirstOrDefault()?.cash_Benefit_SI,
                cash_Benefit_Family_Defn = osRNEData.FirstOrDefault()?.cash_Benefit_Family_Defn,
                Cash_Benefit_Premium = osRNEData.FirstOrDefault()?.Cash_Benefit_Premium,
                cash_Benefit_insured_1 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_1,
                cash_Benefit_insured_2 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_2,
                cash_Benefit_insured_3 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_3,
                cash_Benefit_insured_4 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_4,
                cash_Benefit_insured_5 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_5,
                cash_Benefit_insured_6 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_6,
                cash_Benefit_insured_7 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_7,
                cash_Benefit_insured_8 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_8,
                cash_Benefit_insured_9 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_9,
                cash_Benefit_insured_10 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_10,
                cash_Benefit_insured_11 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_11,
                cash_Benefit_insured_12 = osRNEData.FirstOrDefault()?.cash_Benefit_insured_12,
                cash_Benefit_Premium_Check = osRNEData.FirstOrDefault()?.cash_Benefit_Premium_Check,

                loading_insured_1 = osRNEData.FirstOrDefault()?.loading_insured_1,
                loading_insured_2 = osRNEData.FirstOrDefault()?.loading_insured_2,
                loading_insured_3 = osRNEData.FirstOrDefault()?.loading_insured_3,
                loading_insured_4 = osRNEData.FirstOrDefault()?.loading_insured_4,
                loading_insured_5 = osRNEData.FirstOrDefault()?.loading_insured_5,
                loading_insured_6 = osRNEData.FirstOrDefault()?.loading_insured_6,
                loading_insured_7 = osRNEData.FirstOrDefault()?.loading_insured_7,
                loading_insured_8 = osRNEData.FirstOrDefault()?.loading_insured_8,
                loading_insured_9 = osRNEData.FirstOrDefault()?.loading_insured_9,
                loading_insured_10 = osRNEData.FirstOrDefault()?.loading_insured_10,
                loading_insured_11 = osRNEData.FirstOrDefault()?.loading_insured_11,
                loading_insured_12 = osRNEData.FirstOrDefault()?.loading_insured_12,

                critical_Illness_AddOn_Premium = osRNEData.FirstOrDefault()?.critical_Illness_AddOn_Premium,
                critical_Illness_Add_On_Opt = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Opt,
                critical_Illness_Add_On_SI = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_SI,
                critical_Illness_Add_On_Premium1 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium1,
                critical_Illness_Add_On_Premium2 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium2,
                critical_Illness_Add_On_Premium3 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium3,
                critical_Illness_Add_On_Premium4 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium4,
                critical_Illness_Add_On_Premium5 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium5,
                critical_Illness_Add_On_Premium6 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium6,
                critical_Illness_Add_On_Premium7 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium7,
                critical_Illness_Add_On_Premium8 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium8,
                critical_Illness_Add_On_Premium9 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium9,
                critical_Illness_Add_On_Premium10 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium10,
                critical_Illness_Add_On_Premium11 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium11,
                critical_Illness_Add_On_Premium12 = osRNEData.FirstOrDefault()?.critical_Illness_Add_On_Premium12,
                ci_Variant = osRNEData.FirstOrDefault()?.ci_Variant,

                cash_Benefit_Opt = osRNEData.FirstOrDefault()?.cash_Benefit_Opt,

                base_Loading_And_Discount_Final_BasePremium = osRNEData.FirstOrDefault()?.base_Loading_And_Discount_Final_BasePremium,
                base_Loading_And_Discount_Premium = osRNEData.FirstOrDefault()?.base_Loading_And_Discount_Premium,
                net_premium = osRNEData.FirstOrDefault()?.net_premium,

                num_tot_premium = osRNEData.FirstOrDefault()?.num_tot_premium.HasValue == true ?
                                  (decimal?)Math.Round(osRNEData.FirstOrDefault().num_tot_premium.Value, 2)
                                  : (decimal?)null,

                finalPremium = osRNEData.FirstOrDefault()?.finalPremium.HasValue == true ?
                                  (decimal?)Math.Round(osRNEData.FirstOrDefault().finalPremium.Value, 2)
                                  : (decimal?)null,
                GST = osRNEData.FirstOrDefault()?.GST.HasValue == true ?
                                  (decimal?)Math.Round(osRNEData.FirstOrDefault().GST.Value, 2)
                                  : (decimal?)null,                
                //cross_Check1 = osRNEDataUpSell.FirstOrDefault()?.baseprem_cross_Check ?? 0,
                //cross_Check2 = osRNEDataUpSell.FirstOrDefault()?.upsellbaseprem_cross_Check.HasValue == true
                //              ? (decimal?)Math.Round(osRNEDataUpSell.FirstOrDefault().crossCheck.Value, 2)
                //              : (decimal?)null,

                //upsell premiums and sum insureds
                upsell_sum_insured1 = osRNEDataUpSell.FirstOrDefault()?.sum_insured1,
                upsell_sum_insured2 = osRNEDataUpSell.FirstOrDefault()?.sum_insured2,
                upsell_sum_insured3 = osRNEDataUpSell.FirstOrDefault()?.sum_insured3,
                upsell_sum_insured4 = osRNEDataUpSell.FirstOrDefault()?.sum_insured4,
                upsell_sum_insured5 = osRNEDataUpSell.FirstOrDefault()?.sum_insured5,
                upsell_sum_insured6 = osRNEDataUpSell.FirstOrDefault()?.sum_insured6,

                base_upsell_Premium1 = osRNEDataUpSell.FirstOrDefault()?.base_premium_1,
                base_upsell_Premium2 = osRNEDataUpSell.FirstOrDefault()?.base_premium_2,
                base_upsell_Premium3 = osRNEDataUpSell.FirstOrDefault()?.base_premium_3,
                base_upsell_Premium4 = osRNEDataUpSell.FirstOrDefault()?.base_premium_4,
                base_upsell_Premium5 = osRNEDataUpSell.FirstOrDefault()?.base_premium_5,
                base_upsell_Premium6 = osRNEDataUpSell.FirstOrDefault()?.base_premium_6,

                final_Premium_upsell = osRNEDataUpSell.FirstOrDefault()?.final_Premium_upsell
            };
            decimal? crosscheck1 = osRNEDataUpSell.FirstOrDefault()?.baseprem_cross_Check;
            decimal? crosscheck2 = osRNEDataUpSell.FirstOrDefault()?.upsellbaseprem_cross_Check;
            decimal? netPremium = osRNEDataUpSell.FirstOrDefault()?.netPremium;

            decimal? finalPremium = osRNEDataUpSell.FirstOrDefault()?.final_Premium_upsell;

            decimal? gst = osRNEDataUpSell.FirstOrDefault()?.GST;

            if (objOptimaSecurePremiumValidationUpSell?.policy_number == null)
            {
                Console.WriteLine("Policy number not found.",policyNo);
            }
            string? connectionString = ConfigurationManager.ConnectionStrings["PostgresDb"].ConnectionString;
            using (IDbConnection dbConnection = new NpgsqlConnection(connectionString))
            {
                dbConnection.Open();
                // Check if the record exists by selecting only required columns
                var record_idst = dbConnection.QueryFirstOrDefault<premium_validation>(
                    "SELECT certificate_no FROM ins.premium_validation WHERE certificate_no = @CertificateNo",
                    new { CertificateNo = policyNo });

                if (record_idst == null)
                {
                    if (objOptimaSecurePremiumValidationUpSell.insured_cb1 == string.Empty && objOptimaSecurePremiumValidationUpSell.insured_cb1 == null)
                    {
                        var insertQuery = @"
                    INSERT INTO ins.premium_validation (certificate_no, verified_prem, verified_gst, verified_total_prem, rn_generation_status, final_remarks, dispatch_status)
                    VALUES (@CertificateNo, @VerifiedPrem, @VerifiedGst, @VerifiedTotalPrem, 'IT Issue - No CB', 'CB SI cannot be zero')";

                        dbConnection.Execute(insertQuery, new
                        {
                            CertificateNo = policyNo,
                            VerifiedPrem = netPremium,
                            VerifiedGst = gst,
                            VerifiedTotalPrem = finalPremium
                        });
                        //record_idst.rn_generation_status = "IT Issue - No CB";
                        //record_idst.error_description = "CB SI cannot be zero";
                        // Update the existing record
                        //var updateQuery = @"
                        //    UPDATE ins.premium_validation
                        //    SET rn_generation_status=@RNGenerationStatus
                        //        error_description = @ErrorDescription
                        //    WHERE certificate_no = @CertificateNo";

                        //dbConnection.Execute(updateQuery, new
                        //{
                        //    RNGenerationStatus = record_idst.rn_generation_status,
                        //    ErrorDescription = record_idst.error_description
                        //});
                    }
                    else
                    {
                        try
                        {
                            await HandleCrosschecksAndUpdateStatus(policyNo, osRNEData.FirstOrDefault(), crosscheck1, crosscheck2, netPremium, finalPremium, gst);
                        }
                        catch (DbUpdateConcurrencyException ex)
                        {
                            var entry = ex.Entries.Single();
                            await entry.ReloadAsync();
                        }
                        catch (DbUpdateException ex) when (ex.InnerException is PostgresException pgEx && pgEx.SqlState == "40P01")
                        {

                        }
                       
                    }
                }

                // Check if the record exists by selecting only required columns
                //var record = dbConnection.QueryFirstOrDefault<rne_calculated_cover_rg>(
                //    "SELECT certificate_no FROM ins.rne_calculated_cover_rg WHERE policy_number = @CertificateNo",
                //    new { CertificateNo = policyNo });                
                if (objOptimaSecurePremiumValidationUpSell != null)
                {
                    var no_of_members = objOptimaSecurePremiumValidationUpSell.no_of_members;
                    var ridercount = 3;
                    var policy_number = objOptimaSecurePremiumValidationUpSell.policy_number;
                    var reference_number = objOptimaSecurePremiumValidationUpSell.reference_num;
                    var newRecord = new List<rne_calculated_cover_rg>();
                    for (int i = 1; i <= no_of_members; i++)
                    {
                        var sumInsured = Convert.ToDecimal(objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"sum_insured{i}")?.GetValue(objOptimaSecurePremiumValidationUpSell));
                        var basePremium = Convert.ToDecimal(objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"basePremium{i}")?.GetValue(objOptimaSecurePremiumValidationUpSell));
                        var sumInsuredupsell = Convert.ToDecimal(objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"upsell_sum_insured{i}")?.GetValue(objOptimaSecurePremiumValidationUpSell));
                        var basePremiumupsell = Convert.ToDecimal(objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"base_upsell_Premium{i}")?.GetValue(objOptimaSecurePremiumValidationUpSell));
                        var finalPremiumupsell = Convert.ToDecimal(objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"final_Premium_upsell")?.GetValue(objOptimaSecurePremiumValidationUpSell));

                        if (no_of_members > 1 && i >= 2 && i <= 6)
                        {
                            basePremium *= 0.45m;
                            basePremiumupsell *= 0.45m;
                        }
                        var newRecord1 = new rne_calculated_cover_rg
                        {
                            policy_number = policy_number,
                            referencenum = reference_number,
                            suminsured = sumInsured,
                            premium = basePremium,
                            riskname = objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"txt_insuredname{i}")?.GetValue(objOptimaSecurePremiumValidationUpSell)?.ToString(),
                            covername = "Basic Optima Secure Cover"
                        };
                        var newRecord2 = new rne_calculated_cover_rg
                        {
                            isupsell = 1,
                            policy_number = policy_number,
                            referencenum = reference_number,
                            suminsured = sumInsuredupsell,
                            premium = basePremiumupsell,
                            totalpremium = finalPremiumupsell,//total premium column in rne_calculated_cover_rg will store the finalpremiumupsell from premium computation
                            riskname = objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"txt_insuredname{i}")?.GetValue(objOptimaSecurePremiumValidationUpSell)?.ToString(),
                            covername = "Upsell Cover"
                        };
                        newRecord.Add(newRecord1);
                        newRecord.Add(newRecord2);
                    }

                    for (int j = 1; j <= ridercount; j++)
                    {
                        //var riderSumInsured = Convert.ToDecimal(objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"criticalAdvantageRider_SumInsured_{j}")?.GetValue(objOptimaSecurePremiumValidationUpSell));
                        var riderPremium = Convert.ToDecimal(objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"critical_Illness_Add_On_PremiumList{j}")?.GetValue(objOptimaSecurePremiumValidationUpSell));
                        var riderPremiumpr = Convert.ToDecimal(objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"cash_Benefit_insured_{j}")?.GetValue(objOptimaSecurePremiumValidationUpSell));
                        var riderPremiumur = Convert.ToDecimal(objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"baseAndLoading_Unlimited_Restore")?.GetValue(objOptimaSecurePremiumValidationUpSell));

                        //to print Critical Advantage rider
                        if (riderPremium > 0)
                        {
                            var riderRecord = new rne_calculated_cover_rg
                            {
                                policy_number = policy_number,
                                referencenum = reference_number,
                                //to print Critical Advantage rider
                                riskname = objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"txt_insuredname{j}")?.GetValue(objOptimaSecurePremiumValidationUpSell)?.ToString(),
                                premium = riderPremium,
                                covername = "my:health Critical illness Add on"
                            };
                            newRecord.Add(riderRecord);
                        }

                        //to print Protector Rider
                        if (riderPremiumpr > 0)
                        {
                            var riderRecordpr = new rne_calculated_cover_rg
                            {
                                policy_number = policy_number,
                                referencenum = reference_number,
                                //to print Critical Advantage rider
                                riskname = objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"txt_insuredname{j}")?.GetValue(objOptimaSecurePremiumValidationUpSell)?.ToString(),
                                premium = riderPremiumpr,
                                covername = "my:health Hospital Cash Benefit Add On"
                            };
                            newRecord.Add(riderRecordpr);
                        }

                        //// to print Unlimited Restore
                        if (riderPremiumur > 0)
                        {
                            var riderRecordur = new rne_calculated_cover_rg
                            {
                                policy_number = policy_number,
                                referencenum = reference_number,
                                //to print Hospital Daily Cash Rider
                                riskname = objOptimaSecurePremiumValidationUpSell.GetType().GetProperty($"txt_insuredname{j}")?.GetValue(objOptimaSecurePremiumValidationUpSell)?.ToString(),
                                premium = riderPremiumur,
                                covername = "Unlimited Restore"
                            };
                            newRecord.Add(riderRecordur);
                        }
                    }
                    var insertQuery = @"
    INSERT INTO rne_calculated_cover_rg (policy_number, referencenum, suminsured, premium, totalpremium, riskname, covername, isupsell)
    VALUES (@policy_number, @referencenum, @suminsured, @premium, @totalpremium, @riskname, @covername, @isupsell);
    ";

                    // Using ExecuteAsync for asynchronous execution
                    await dbConnection.ExecuteAsync(insertQuery, newRecord).ConfigureAwait(false);
                    //dbContext.rne_calculated_cover_rg.AddRange(newRecord);
                    //await dbContext.SaveChangesAsync();

                }

                return objOptimaSecurePremiumValidationUpSell;
            }
        }
        async Task<verifiedpremiumvalues> GetCrosscheckValue(string policyNo, List<OptimaRestoreRNE> orRNEData)
        {
            string? connectionString = ConfigurationManager.ConnectionStrings["PostgresDb"].ConnectionString;
            using (IDbConnection dbConnection = new NpgsqlConnection(connectionString))
            {
                dbConnection.Open();
                // Check if the record exists by selecting only required columns
                var record = dbConnection.QueryFirstOrDefault<premium_validation>(
                    "SELECT certificate_no FROM ins.premium_validation WHERE certificate_no = @CertificateNo",
                    new { CertificateNo = policyNo.ToString() });

                if (record != null && record.rn_generation_status == null)
                {
                    decimal? crosscheck1 = (orRNEData.FirstOrDefault().num_tot_premium - record.verified_total_prem);
                    return new verifiedpremiumvalues
                    {
                        verified_gst = record.verified_gst ?? 0,  // Handle possible nulls
                        verified_total_premium = record.verified_total_prem ?? 0,
                        verified_net_premium = record.verified_prem ?? 0,
                        crosscheck = crosscheck1
                    };

                }
                else
                {
                    return new verifiedpremiumvalues
                    {
                        verified_gst = 0,  // Default values
                        verified_total_premium = 0,
                        verified_net_premium = 0,
                        crosscheck = null
                    };
                }

            }
        }
        async Task HandleCrosschecksAndUpdateStatus(string policyNo,OptimaSecureRNE osRNEData, decimal? crosscheck1, decimal? crosscheck2, decimal? netPremium, decimal? finalPremium, decimal? gst)
        {
            string? connectionString = ConfigurationManager.ConnectionStrings["PostgresDb"].ConnectionString;
            using (IDbConnection dbConnection = new NpgsqlConnection(connectionString))
            {
                dbConnection.Open();
                // Check if the record exists by selecting only required columns
                var record = dbConnection.QueryFirstOrDefault<premium_validation>(
                    "SELECT certificate_no FROM ins.premium_validation WHERE certificate_no = @CertificateNo",
                    new { CertificateNo = osRNEData.policy_number.ToString() });

                if (record == null)
                {
                    decimal crosscheck1Value = crosscheck1.HasValue ? crosscheck1.Value : 0;
                    decimal crosscheck2Value = crosscheck2.HasValue ? crosscheck2.Value : 0;
                    if (crosscheck1.HasValue && crosscheck2.HasValue)
                    {
                        if ((Math.Abs(crosscheck1.Value) <= 10) || ((Math.Abs(crosscheck1.Value) <= 10 && Math.Abs(crosscheck2Value) <= 10)))
                        {
                            var insertQuery = @"
                    INSERT INTO ins.premium_validation (certificate_no, verified_prem, verified_gst, verified_total_prem, rn_generation_status, final_remarks, dispatch_status)
                    VALUES (@CertificateNo, @VerifiedPrem, @VerifiedGst, @VerifiedTotalPrem, 'RN Generation Awaited', 'RN Generation Awaited', 'PDF Gen Under Process With CLICK PSS Team')";

                            dbConnection.Execute(insertQuery, new
                            {
                                CertificateNo = osRNEData.policy_number.ToString(),
                                VerifiedPrem = netPremium,
                                VerifiedGst = gst,
                                VerifiedTotalPrem = finalPremium
                            });
                            //record.rn_generation_status = "RN Generation Awaited";
                            //record.final_remarks = "RN Generation Awaited";
                            //record.dispatch_status = "PDF Gen Under Process With CLICK PSS Team";
                        }
                        else if ((Math.Abs(crosscheck1.Value) > 10) || (Math.Abs(crosscheck1.Value) > 10 && Math.Abs(crosscheck2Value) <= 10))
                        {
                            var insertQuery = @"
                            INSERT INTO ins.premium_validation (certificate_no, verified_prem, verified_gst, verified_total_prem, rn_generation_status, final_remarks, dispatch_status, error_description)
                            VALUES (@CertificateNo, @VerifiedPrem, @VerifiedGst, @VerifiedTotalPrem, 'IT Issue - QC Failed', 'IT Issues', 'Revised Extraction REQ From IT Team QC Failed Cases', 'Premium verification failed due to premium difference of more than 10 rupees')";

                            dbConnection.Execute(insertQuery, new
                            {
                                CertificateNo = osRNEData.policy_number.ToString(),
                                VerifiedPrem = netPremium,
                                VerifiedGst = gst,
                                VerifiedTotalPrem = finalPremium,
                            });
                            //record.rn_generation_status = "IT Issue - QC Failed";
                            //record.final_remarks = "IT Issues";
                            //record.dispatch_status = "Revised Extraction REQ From IT Team QC Failed Cases";
                            //record.error_description = "Premium verification failed due to premium difference of more than 10 rupees";
                        }
                        else if (Math.Abs(crosscheck1.Value) <= 10 && Math.Abs(crosscheck2Value) > 10)
                        {
                            var insertQuery = @"
                            INSERT INTO ins.premium_validation (certificate_no, verified_prem, verified_gst, verified_total_prem, rn_generation_status, final_remarks, dispatch_status, error_description)
                            VALUES (@CertificateNo, @VerifiedPrem, @VerifiedGst, @VerifiedTotalPrem, 'IT Issue - Upsell QC Failed')";

                            dbConnection.Execute(insertQuery, new
                            {
                                CertificateNo = osRNEData.policy_number.ToString(),
                                VerifiedPrem = netPremium,
                                VerifiedGst = gst,
                                VerifiedTotalPrem = finalPremium,
                            });

                            //record.rn_generation_status = "IT Issue - Upsell QC Failed";
                        }
                        else if (Math.Abs(crosscheck1.Value) > 10 && Math.Abs(crosscheck2Value) > 10)
                        {
                            // record.rn_generation_status = "IT Issue - QC Failed";

                            var insertQuery = @"
                            INSERT INTO ins.premium_validation (certificate_no, verified_prem, verified_gst, verified_total_prem, rn_generation_status, final_remarks, dispatch_status, error_description)
                            VALUES (@CertificateNo, @VerifiedPrem, @VerifiedGst, @VerifiedTotalPrem, 'IT Issue - QC Failed')";

                            dbConnection.Execute(insertQuery, new
                            {
                                CertificateNo = osRNEData.policy_number.ToString(),
                                VerifiedPrem = netPremium,
                                VerifiedGst = gst,
                                VerifiedTotalPrem = finalPremium,
                            });
                        }
                    }
                 
                }
                
            }
        }

        //private async Task<IEnumerable<OptimaSecureRNE>> GetGCOptimaSecureDataAsync(string policyNo, ILogger _logger, HDFCDbContext dbContext, int pageNumber, int pageSize)
        //{            
        //    var osRNEData = await (
        //    from os in dbContext.rne_healthtab
        //    join osidst in dbContext.idst_renewal_data_rgs on os.policy_number equals osidst.certificate_no
        //    where (os.policy_number == policyNo)
        //    select new OptimaSecureRNE
        //    {
        //        prod_code = os.prod_code,
        //        batchid = os.batchid,
        //        reference_num = os.reference_num,
        //        prod_name = os.prod_name,
        //        policy_number = os.policy_number,
        //        split_flag = os.split_flag,
        //        customer_id = os.customer_id,
        //        customername = os.customername,
        //        verticalname = os.verticalname,//psm name in old gc mapping
        //        policy_start_date = os.policy_start_date,
        //        policy_expiry_date = os.policy_expiry_date,
        //        policy_period = os.policy_period,
        //        tier_type = os.tier_type,//tier in old gc mapping
        //        policyplan = os.policyplan,
        //        policy_type = os.policy_type,
        //        txt_family = os.txt_family,//family size in old gc mapping
        //        claimcount = os.claimcount,
        //        num_tot_premium = os.num_tot_premium,
        //        num_net_premium = os.num_net_premium,
        //        optima_secure_gst = os.num_service_tax,
               
        //        txt_insured_entrydate1 = os.txt_insured_entrydate1,
        //        txt_insured_entrydate2 = os.txt_insured_entrydate2,
        //        txt_insured_entrydate3 = os.txt_insured_entrydate3,
        //        txt_insured_entrydate4 = os.txt_insured_entrydate4,
        //        txt_insured_entrydate5 = os.txt_insured_entrydate5,
        //        txt_insured_entrydate6 = os.txt_insured_entrydate6,
        //        txt_insured_entrydate7 = os.txt_insured_entrydate7,
        //        txt_insured_entrydate8 = os.txt_insured_entrydate8,
        //        txt_insured_entrydate9 = os.txt_insured_entrydate9,
        //        txt_insured_entrydate10 = os.txt_insured_entrydate10,
        //        txt_insured_entrydate11 = os.txt_insured_entrydate11,
        //        txt_insured_entrydate12 = os.txt_insured_entrydate12,

        //        coverbaseloadingrate1 = os.coverbaseloadingrate1,
        //        coverbaseloadingrate2 = os.coverbaseloadingrate2,
        //        coverbaseloadingrate3 = os.coverbaseloadingrate3,
        //        coverbaseloadingrate4 = os.coverbaseloadingrate4,
        //        coverbaseloadingrate5 = os.coverbaseloadingrate5,
        //        coverbaseloadingrate6 = os.coverbaseloadingrate6,
        //        coverbaseloadingrate7 = os.coverbaseloadingrate7,
        //        coverbaseloadingrate8 = os.coverbaseloadingrate8,
        //        coverbaseloadingrate9 = os.coverbaseloadingrate9,
        //        coverbaseloadingrate10 = os.coverbaseloadingrate10,
        //        coverbaseloadingrate11 = os.coverbaseloadingrate11,
        //        coverbaseloadingrate12 = os.coverbaseloadingrate12,

        //        insured_loadingper1 = osidst.loading_per_insured1,
        //        insured_loadingper2 = osidst.loading_per_insured2,
        //        insured_loadingper3 = osidst.loading_per_insured3,
        //        insured_loadingper4 = osidst.loading_per_insured4,
        //        insured_loadingper5 = osidst.loading_per_insured5,
        //        insured_loadingper6 = osidst.loading_per_insured6,
        //        insured_loadingper7 = osidst.loading_per_insured7,
        //        insured_loadingper8 = osidst.loading_per_insured8,
        //        insured_loadingper9 = osidst.loading_per_insured9,
        //        insured_loadingper10 = osidst.loading_per_insured10,
        //        insured_loadingper11 = osidst.loading_per_insured11,
        //        insured_loadingper12 = osidst.loading_per_insured12,

        //        insured_loadingamt1 = os.insured_loadingamt1,
        //        insured_loadingamt2 = os.insured_loadingamt2,
        //        insured_loadingamt3 = os.insured_loadingamt3,
        //        insured_loadingamt4 = os.insured_loadingamt4,
        //        insured_loadingamt5 = os.insured_loadingamt5,
        //        insured_loadingamt6 = os.insured_loadingamt6,
        //        insured_loadingamt7 = os.insured_loadingamt7,
        //        insured_loadingamt8 = os.insured_loadingamt8,
        //        insured_loadingamt9 = os.insured_loadingamt9,
        //        insured_loadingamt10 = os.insured_loadingamt10,
        //        insured_loadingamt11 = os.insured_loadingamt11,
        //        insured_loadingamt12 = os.insured_loadingamt12,

        //        txt_insuredname1 = os.txt_insuredname1,
        //        txt_insuredname2 = os.txt_insuredname2,
        //        txt_insuredname3 = os.txt_insuredname3,
        //        txt_insuredname4 = os.txt_insuredname4,
        //        txt_insuredname5 = os.txt_insuredname5,
        //        txt_insuredname6 = os.txt_insuredname6,
        //        txt_insuredname7 = os.txt_insuredname7,
        //        txt_insuredname8 = os.txt_insuredname8,
        //        txt_insuredname9 = os.txt_insuredname9,
        //        txt_insuredname10 = os.txt_insuredname10,
        //        txt_insuredname11 = os.txt_insuredname11,
        //        txt_insuredname12 = os.txt_insuredname12,

        //        txt_insured_relation1 = os.txt_insured_relation1,
        //        txt_insured_relation2 = os.txt_insured_relation2,
        //        txt_insured_relation3 = os.txt_insured_relation3,
        //        txt_insured_relation4 = os.txt_insured_relation4,
        //        txt_insured_relation5 = os.txt_insured_relation5,
        //        txt_insured_relation6 = os.txt_insured_relation6,
        //        txt_insured_relation7 = os.txt_insured_relation7,
        //        txt_insured_relation8 = os.txt_insured_relation8,
        //        txt_insured_relation9 = os.txt_insured_relation9,
        //        txt_insured_relation10 = os.txt_insured_relation10,
        //        txt_insured_relation11 = os.txt_insured_relation11,
        //        txt_insured_relation12 = os.txt_insured_relation12,

        //        txt_insured_age1 = os.txt_insured_age1,
        //        txt_insured_age2 = os.txt_insured_age2,
        //        txt_insured_age3 = os.txt_insured_age3,
        //        txt_insured_age4 = os.txt_insured_age4,
        //        txt_insured_age5 = os.txt_insured_age5,
        //        txt_insured_age6 = os.txt_insured_age6,
        //        txt_insured_age7 = os.txt_insured_age7,
        //        txt_insured_age8 = os.txt_insured_age8,
        //        txt_insured_age9 = os.txt_insured_age9,
        //        txt_insured_age10 = os.txt_insured_age10,
        //        txt_insured_age11 = os.txt_insured_age11,
        //        txt_insured_age12 = os.txt_insured_age12,

        //        member_id1 = os.member_id1,
        //        member_id2 = os.member_id2,
        //        member_id3 = os.member_id3,
        //        member_id4 = os.member_id4,
        //        member_id5 = os.member_id5,
        //        member_id6 = os.member_id6,
        //        member_id7 = os.member_id7,
        //        member_id8 = os.member_id8,
        //        member_id9 = os.member_id9,
        //        member_id10 = os.member_id10,
        //        member_id11 = os.member_id11,
        //        member_id12 = os.member_id12,

        //        pollddesc1 = os.pollddesc1,
        //        pollddesc2 = os.pollddesc2,
        //        pollddesc3 = os.pollddesc3,
        //        pollddesc4 = os.pollddesc4,
        //        pollddesc5 = os.pollddesc5,

        //        upselltype1 = os.upselltype1,
        //        upselltype2 = os.upselltype2,
        //        upselltype3 = os.upselltype3,
        //        upselltype4 = os.upselltype4,
        //        upselltype5 = os.upselltype5,

        //        upsellvalue1 = os.upsellvalue1,
        //        upsellvalue2 = os.upsellvalue2,
        //        upsellvalue3 = os.upsellvalue3,
        //        upsellvalue4 = os.upsellvalue4,
        //        upsellvalue5 = os.upsellvalue5,

        //        upsellpremium1 = os.upsellpremium1,
        //        upsellpremium2 = os.upsellpremium2,
        //        upsellpremium3 = os.upsellpremium3,
        //        upsellpremium4 = os.upsellpremium4,
        //        upsellpremium5 = os.upsellpremium5,

        //        sum_insured1 = os.sum_insured1,
        //        sum_insured2 = os.sum_insured2,
        //        sum_insured3 = os.sum_insured3,
        //        sum_insured4 = os.sum_insured4,
        //        sum_insured5 = os.sum_insured5,
        //        sum_insured6 = os.sum_insured6,
        //        sum_insured7 = os.sum_insured7,
        //        sum_insured8 = os.sum_insured8,
        //        sum_insured9 = os.sum_insured9,
        //        sum_insured10 = os.sum_insured10,
        //        sum_insured11 = os.sum_insured11,
        //        sum_insured12 = os.sum_insured12,

        //        insured_cb1 = os.insured_cb1,
        //        insured_cb2 = os.insured_cb2,
        //        insured_cb3 = os.insured_cb3,
        //        insured_cb4 = os.insured_cb4,
        //        insured_cb5 = os.insured_cb5,
        //        insured_cb6 = os.insured_cb6,
        //        insured_cb7 = os.insured_cb7,
        //        insured_cb8 = os.insured_cb8,
        //        insured_cb9 = os.insured_cb9,
        //        insured_cb10 = os.insured_cb10,
        //        insured_cb11 = os.insured_cb11,
        //        insured_cb12 = os.insured_cb12,

        //        insured_deductable1 = os.insured_deductable1,
        //        insured_deductable2 = os.insured_deductable2,
        //        insured_deductable3 = os.insured_deductable3,
        //        insured_deductable4 = os.insured_deductable4,
        //        insured_deductable5 = os.insured_deductable5,
        //        insured_deductable6 = os.insured_deductable6,
        //        insured_deductable7 = os.insured_deductable7,
        //        insured_deductable8 = os.insured_deductable8,
        //        insured_deductable9 = os.insured_deductable9,
        //        insured_deductable10 = os.insured_deductable10,
        //        insured_deductable11 = os.insured_deductable11,
        //        insured_deductable12 = os.insured_deductable12,

        //        covername11 = os.covername11,
        //        covername12 = os.covername12,
        //        covername13 = os.covername13,
        //        covername14 = os.covername14,
        //        covername15 = os.covername15,
        //        covername16 = os.covername16,
        //        covername17 = os.covername17,
        //        covername18 = os.covername18,
        //        covername19 = os.covername19,
        //        covername21 = os.covername21,
        //        covername22 = os.covername22,
        //        covername23 = os.covername23,
        //        covername24 = os.covername24,
        //        covername25 = os.covername25,
        //        covername26 = os.covername26,
        //        covername27 = os.covername27,
        //        covername28 = os.covername28,
        //        covername29 = os.covername29,
        //        covername31 = os.covername31,
        //        covername32 = os.covername32,
        //        covername33 = os.covername33,
        //        covername34 = os.covername34,
        //        covername35 = os.covername35,
        //        covername36 = os.covername36,
        //        covername37 = os.covername37,
        //        covername38 = os.covername38,
        //        covername39 = os.covername39,
        //        covername41 = os.covername41,
        //        covername42 = os.covername42,
        //        covername43 = os.covername43,
        //        covername44 = os.covername44,
        //        covername45 = os.covername45,
        //        covername46 = os.covername46,
        //        covername47 = os.covername47,
        //        covername48 = os.covername48,
        //        covername49 = os.covername49,
        //        covername51 = os.covername51,
        //        covername52 = os.covername52,
        //        covername53 = os.covername53,
        //        covername54 = os.covername54,
        //        covername55 = os.covername55,
        //        covername56 = os.covername56,
        //        covername57 = os.covername57,
        //        covername58 = os.covername58,
        //        covername59 = os.covername59,
        //        covername61 = os.covername61,
        //        covername62 = os.covername62,
        //        covername63 = os.covername63,
        //        covername64 = os.covername64,
        //        covername65 = os.covername65,
        //        covername66 = os.covername66,
        //        covername67 = os.covername67,
        //        covername68 = os.covername68,
        //        covername69 = os.covername69,
        //        covername71 = os.covername71,
        //        covername72 = os.covername72,
        //        covername73 = os.covername73,
        //        covername74 = os.covername74,
        //        covername75 = os.covername75,
        //        covername76 = os.covername76,
        //        covername77 = os.covername77,
        //        covername78 = os.covername78,
        //        covername79 = os.covername79,
        //        covername81 = os.covername81,
        //        covername82 = os.covername82,
        //        covername83 = os.covername83,
        //        covername84 = os.covername84,
        //        covername85 = os.covername85,
        //        covername86 = os.covername86,
        //        covername87 = os.covername87,
        //        covername88 = os.covername88,
        //        covername89 = os.covername89,
        //        covername91 = os.covername91,
        //        covername92 = os.covername92,
        //        covername93 = os.covername93,
        //        covername94 = os.covername94,
        //        covername95 = os.covername95,
        //        covername96 = os.covername96,
        //        covername97 = os.covername97,
        //        covername98 = os.covername98,
        //        covername99 = os.covername99,
        //        covername101 = os.covername101,
        //        covername102 = os.covername102,
        //        covername103 = os.covername103,
        //        covername104 = os.covername104,
        //        covername105 = os.covername105,
        //        covername106 = os.covername106,
        //        covername107 = os.covername107,
        //        covername108 = os.covername108,
        //        covername109 = os.covername109,
        //        covername110 = os.covername110,
        //        covername210 = os.covername210,
        //        covername310 = os.covername310,
        //        covername410 = os.covername410,
        //        covername510 = os.covername510,
        //        covername610 = os.covername610,
        //        covername710 = os.covername710,
        //        covername810 = os.covername810,
        //        covername910 = os.covername910,
        //        covername1010 = os.covername1010,

        //        coversi11 = os.coversi11,
        //        coversi12 = os.coversi12,
        //        coversi13 = os.coversi13,
        //        coversi14 = os.coversi14,
        //        coversi15 = os.coversi15,
        //        coversi16 = os.coversi16,
        //        coversi17 = os.coversi17,
        //        coversi18 = os.coversi18,
        //        coversi19 = os.coversi19,
        //        coversi21 = os.coversi21,
        //        coversi22 = os.coversi22,
        //        coversi23 = os.coversi23,
        //        coversi24 = os.coversi24,
        //        coversi25 = os.coversi25,
        //        coversi26 = os.coversi26,
        //        coversi27 = os.coversi27,
        //        coversi28 = os.coversi28,
        //        coversi29 = os.coversi29,
        //        coversi31 = os.coversi31,
        //        coversi32 = os.coversi32,
        //        coversi33 = os.coversi33,
        //        coversi34 = os.coversi34,
        //        coversi35 = os.coversi35,
        //        coversi36 = os.coversi36,
        //        coversi37 = os.coversi37,
        //        coversi38 = os.coversi38,
        //        coversi39 = os.coversi39,
        //        coversi41 = os.coversi41,
        //        coversi42 = os.coversi42,
        //        coversi43 = os.coversi43,
        //        coversi44 = os.coversi44,
        //        coversi45 = os.coversi46,
        //        coversi47 = os.coversi47,
        //        coversi48 = os.coversi48,
        //        coversi49 = os.coversi49,
        //        coversi51 = os.coversi51,
        //        coversi52 = os.coversi52,
        //        coversi53 = os.coversi53,
        //        coversi54 = os.coversi54,
        //        coversi55 = os.coversi55,
        //        coversi56 = os.coversi56,
        //        coversi57 = os.coversi57,
        //        coversi58 = os.coversi58,
        //        coversi59 = os.coversi59,
        //        coversi61 = os.coversi61,
        //        coversi62 = os.coversi62,
        //        coversi63 = os.coversi63,
        //        coversi64 = os.coversi64,
        //        coversi65 = os.coversi65,
        //        coversi66 = os.coversi66,
        //        coversi67 = os.coversi67,
        //        coversi68 = os.coversi68,
        //        coversi69 = os.coversi69,
        //        coversi71 = os.coversi71,
        //        coversi72 = os.coversi72,
        //        coversi73 = os.coversi73,
        //        coversi74 = os.coversi74,
        //        coversi75 = os.coversi75,
        //        coversi76 = os.coversi76,
        //        coversi77 = os.coversi77,
        //        coversi78 = os.coversi78,
        //        coversi79 = os.coversi79,
        //        coversi81 = os.coversi81,
        //        coversi82 = os.coversi82,
        //        coversi83 = os.coversi83,
        //        coversi84 = os.coversi84,
        //        coversi85 = os.coversi85,
        //        coversi86 = os.coversi86,
        //        coversi87 = os.coversi87,
        //        coversi88 = os.coversi88,
        //        coversi89 = os.coversi89,
        //        coversi91 = os.coversi91,
        //        coversi92 = os.coversi92,
        //        coversi93 = os.coversi93,
        //        coversi94 = os.coversi94,
        //        coversi95 = os.coversi95,
        //        coversi96 = os.coversi96,
        //        coversi97 = os.coversi97,
        //        coversi98 = os.coversi98,
        //        coversi99 = os.coversi99,
        //        coversi101 = os.coversi101,
        //        coversi102 = os.coversi102,
        //        coversi103 = os.coversi103,
        //        coversi104 = os.coversi104,
        //        coversi105 = os.coversi105,
        //        coversi106 = os.coversi106,
        //        coversi107 = os.coversi107,
        //        coversi108 = os.coversi108,
        //        coversi109 = os.coversi109,
        //        coversi210 = os.coversi210,
        //        coversi310 = os.coversi310,
        //        coversi410 = os.coversi410,
        //        coversi510 = os.coversi510,
        //        coversi610 = os.coversi610,
        //        coversi810 = os.coversi810,
        //        coversi910 = os.coversi910,
        //        coversi1010 = os.coversi1010,

        //        coverprem11 = os.coverprem11,
        //        coverprem12 = os.coverprem12,
        //        coverprem13 = os.coverprem13,
        //        coverprem14 = os.coverprem14,
        //        coverprem15 = os.coverprem15,
        //        coverprem16 = os.coverprem16,
        //        coverprem17 = os.coverprem17,
        //        coverprem18 = os.coverprem18,
        //        coverprem19 = os.coverprem19,
        //        coverprem21 = os.coverprem21,
        //        coverprem22 = os.coverprem22,
        //        coverprem23 = os.coverprem23,
        //        coverprem24 = os.coverprem24,
        //        coverprem25 = os.coverprem25,
        //        coverprem26 = os.coverprem26,
        //        coverprem27 = os.coverprem27,
        //        coverprem28 = os.coverprem28,
        //        coverprem29 = os.coverprem29,
        //        coverprem31 = os.coverprem31,
        //        coverprem32 = os.coverprem32,
        //        coverprem33 = os.coverprem33,
        //        coverprem34 = os.coverprem34,
        //        coverprem35 = os.coverprem35,
        //        coverprem36 = os.coverprem36,
        //        coverprem37 = os.coverprem37,
        //        coverprem38 = os.coverprem38,
        //        coverprem39 = os.coverprem39,
        //        coverprem41 = os.coverprem41,
        //        coverprem42 = os.coverprem42,
        //        coverprem43 = os.coverprem43,
        //        coverprem44 = os.coverprem44,
        //        coverprem45 = os.coverprem46,
        //        coverprem47 = os.coverprem47,
        //        coverprem48 = os.coverprem48,
        //        coverprem49 = os.coverprem49,
        //        coverprem51 = os.coverprem51,
        //        coverprem52 = os.coverprem52,
        //        coverprem53 = os.coverprem53,
        //        coverprem54 = os.coverprem54,
        //        coverprem55 = os.coverprem55,
        //        coverprem56 = os.coverprem56,
        //        coverprem57 = os.coverprem57,
        //        coverprem58 = os.coverprem58,
        //        coverprem59 = os.coverprem59,
        //        coverprem61 = os.coverprem61,
        //        coverprem62 = os.coverprem62,
        //        coverprem63 = os.coverprem63,
        //        coverprem64 = os.coverprem64,
        //        coverprem65 = os.coverprem65,
        //        coverprem66 = os.coverprem66,
        //        coverprem67 = os.coverprem67,
        //        coverprem68 = os.coverprem68,
        //        coverprem69 = os.coverprem69,
        //        coverprem71 = os.coverprem71,
        //        coverprem72 = os.coverprem72,
        //        coverprem73 = os.coverprem73,
        //        coverprem74 = os.coverprem74,
        //        coverprem75 = os.coverprem75,
        //        coverprem76 = os.coverprem76,
        //        coverprem77 = os.coverprem77,
        //        coverprem78 = os.coverprem78,
        //        coverprem79 = os.coverprem79,
        //        coverprem81 = os.coverprem81,
        //        coverprem82 = os.coverprem82,
        //        coverprem83 = os.coverprem83,
        //        coverprem84 = os.coverprem84,
        //        coverprem85 = os.coverprem85,
        //        coverprem86 = os.coverprem86,
        //        coverprem87 = os.coverprem87,
        //        coverprem88 = os.coverprem88,
        //        coverprem89 = os.coverprem89,
        //        coverprem91 = os.coverprem91,
        //        coverprem92 = os.coverprem92,
        //        coverprem93 = os.coverprem93,
        //        coverprem94 = os.coverprem94,
        //        coverprem95 = os.coverprem95,
        //        coverprem96 = os.coverprem96,
        //        coverprem97 = os.coverprem97,
        //        coverprem98 = os.coverprem98,
        //        coverprem99 = os.coverprem99,
        //        coverprem101 = os.coverprem101,
        //        coverprem102 = os.coverprem102,
        //        coverprem103 = os.coverprem103,
        //        coverprem104 = os.coverprem104,
        //        coverprem105 = os.coverprem105,
        //        coverprem106 = os.coverprem106,
        //        coverprem107 = os.coverprem107,
        //        coverprem108 = os.coverprem108,
        //        coverprem109 = os.coverprem109,
        //        coverprem210 = os.coverprem210,
        //        coverprem310 = os.coverprem310,
        //        coverprem410 = os.coverprem410,
        //        coverprem510 = os.coverprem510,
        //        coverprem610 = os.coverprem610,
        //        coverprem810 = os.coverprem810,
        //        coverprem910 = os.coverprem910,
        //        coverprem1010 = os.coverprem1010,

        //        coverloadingrate11 = os.coverloadingrate11,
        //        coverloadingrate12 = os.coverloadingrate12,
        //        coverloadingrate13 = os.coverloadingrate13,
        //        coverloadingrate14 = os.coverloadingrate14,
        //        coverloadingrate15 = os.coverloadingrate15,
        //        coverloadingrate16 = os.coverloadingrate16,
        //        coverloadingrate17 = os.coverloadingrate17,
        //        coverloadingrate18 = os.coverloadingrate18,
        //        coverloadingrate19 = os.coverloadingrate19,
        //        coverloadingrate21 = os.coverloadingrate21,
        //        coverloadingrate22 = os.coverloadingrate22,
        //        coverloadingrate23 = os.coverloadingrate23,
        //        coverloadingrate24 = os.coverloadingrate24,
        //        coverloadingrate25 = os.coverloadingrate25,
        //        coverloadingrate26 = os.coverloadingrate26,
        //        coverloadingrate27 = os.coverloadingrate27,
        //        coverloadingrate28 = os.coverloadingrate28,
        //        coverloadingrate29 = os.coverloadingrate29,
        //        coverloadingrate31 = os.coverloadingrate31,
        //        coverloadingrate32 = os.coverloadingrate32,
        //        coverloadingrate33 = os.coverloadingrate33,
        //        coverloadingrate34 = os.coverloadingrate34,
        //        coverloadingrate35 = os.coverloadingrate35,
        //        coverloadingrate36 = os.coverloadingrate36,
        //        coverloadingrate37 = os.coverloadingrate37,
        //        coverloadingrate38 = os.coverloadingrate38,
        //        coverloadingrate39 = os.coverloadingrate39,
        //        coverloadingrate41 = os.coverloadingrate41,
        //        coverloadingrate42 = os.coverloadingrate42,
        //        coverloadingrate43 = os.coverloadingrate43,
        //        coverloadingrate44 = os.coverloadingrate44,
        //        coverloadingrate45 = os.coverloadingrate46,
        //        coverloadingrate47 = os.coverloadingrate47,
        //        coverloadingrate48 = os.coverloadingrate48,
        //        coverloadingrate49 = os.coverloadingrate49,
        //        coverloadingrate51 = os.coverloadingrate51,
        //        coverloadingrate52 = os.coverloadingrate52,
        //        coverloadingrate53 = os.coverloadingrate53,
        //        coverloadingrate54 = os.coverloadingrate54,
        //        coverloadingrate55 = os.coverloadingrate55,
        //        coverloadingrate56 = os.coverloadingrate56,
        //        coverloadingrate57 = os.coverloadingrate57,
        //        coverloadingrate58 = os.coverloadingrate58,
        //        coverloadingrate59 = os.coverloadingrate59,
        //        coverloadingrate61 = os.coverloadingrate61,
        //        coverloadingrate62 = os.coverloadingrate62,
        //        coverloadingrate63 = os.coverloadingrate63,
        //        coverloadingrate64 = os.coverloadingrate64,
        //        coverloadingrate65 = os.coverloadingrate65,
        //        coverloadingrate66 = os.coverloadingrate66,
        //        coverloadingrate67 = os.coverloadingrate67,
        //        coverloadingrate68 = os.coverloadingrate68,
        //        coverloadingrate69 = os.coverloadingrate69,
        //        coverloadingrate71 = os.coverloadingrate71,
        //        coverloadingrate72 = os.coverloadingrate72,
        //        coverloadingrate73 = os.coverloadingrate73,
        //        coverloadingrate74 = os.coverloadingrate74,
        //        coverloadingrate75 = os.coverloadingrate75,
        //        coverloadingrate76 = os.coverloadingrate76,
        //        coverloadingrate77 = os.coverloadingrate77,
        //        coverloadingrate78 = os.coverloadingrate78,
        //        coverloadingrate79 = os.coverloadingrate79,
        //        coverloadingrate81 = os.coverloadingrate81,
        //        coverloadingrate82 = os.coverloadingrate82,
        //        coverloadingrate83 = os.coverloadingrate83,
        //        coverloadingrate84 = os.coverloadingrate84,
        //        coverloadingrate85 = os.coverloadingrate85,
        //        coverloadingrate86 = os.coverloadingrate86,
        //        coverloadingrate87 = os.coverloadingrate87,
        //        coverloadingrate88 = os.coverloadingrate88,
        //        coverloadingrate89 = os.coverloadingrate89,
        //        coverloadingrate91 = os.coverloadingrate91,
        //        coverloadingrate92 = os.coverloadingrate92,
        //        coverloadingrate93 = os.coverloadingrate93,
        //        coverloadingrate94 = os.coverloadingrate94,
        //        coverloadingrate95 = os.coverloadingrate95,
        //        coverloadingrate96 = os.coverloadingrate96,
        //        coverloadingrate97 = os.coverloadingrate97,
        //        coverloadingrate98 = os.coverloadingrate98,
        //        coverloadingrate99 = os.coverloadingrate99,
        //        coverloadingrate101 = os.coverloadingrate101,
        //        coverloadingrate102 = os.coverloadingrate102,
        //        coverloadingrate103 = os.coverloadingrate103,
        //        coverloadingrate104 = os.coverloadingrate104,
        //        coverloadingrate105 = os.coverloadingrate105,
        //        coverloadingrate106 = os.coverloadingrate106,
        //        coverloadingrate107 = os.coverloadingrate107,
        //        coverloadingrate108 = os.coverloadingrate108,
        //        coverloadingrate109 = os.coverloadingrate109,
        //        coverloadingrate210 = os.coverloadingrate210,
        //        coverloadingrate310 = os.coverloadingrate310,
        //        coverloadingrate410 = os.coverloadingrate410,
        //        coverloadingrate510 = os.coverloadingrate510,
        //        coverloadingrate610 = os.coverloadingrate610,
        //        coverloadingrate810 = os.coverloadingrate810,
        //        coverloadingrate910 = os.coverloadingrate910,
        //        coverloadingrate1010 = os.coverloadingrate1010,
        //    }
        //    ).Skip((pageNumber - 1) * pageSize) // Skip the appropriate number of records
        //    .Take(pageSize) // Take only the specified page size
        //.ToListAsync();
        //    return new List<OptimaSecureRNE>(osRNEData);
        //}
        static Dictionary<string, string> DataRowToDictionary(DataRow row)
        {
            var dictionary = new Dictionary<string, string>();

            foreach (DataColumn column in row.Table.Columns)
            {
                dictionary[column.ColumnName] = row[column].ToString();
            }

            return dictionary;
        }
        public static Dictionary<string, object> ExtractData(OptimaSecureUpsellPremiumValidation.Models.Domain.OptimaSecureRNE optimaGC)
        {
            var data = new Dictionary<string, object>();

            // Get all properties of the OptimaGC class
            var properties = typeof(OptimaSecureUpsellPremiumValidation.Models.Domain.OptimaSecureRNE).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            // Iterate over each property
            foreach (var property in properties)
            {
                for (int i = 11; i <= 1010; i += 1)  // Adjust the range if necessary
                {
                    // Check if the property name matches the pattern
                    if (property.Name.StartsWith($"covername{i}") ||
                         property.Name.StartsWith($"coversi{i}") ||
                         property.Name.StartsWith($"coverprem{i}") ||
                         property.Name.StartsWith($"coverloadingrate{i}"))
                    {
                        // Add property to dictionary
                        data[property.Name] = property.GetValue(optimaGC);
                    }
                }
            }

            return data;
        }
        public async Task<IEnumerable<IdstData>> GetIdstRenewalData(string policyNo)
        {
            string? connectionString = ConfigurationManager.ConnectionStrings["PostgresDb"].ConnectionString;
            string sqlQuery = @"
            SELECT 
              certificate_no,
              loading_per_insured1,
              loading_per_insured2,
              loading_per_insured3,
              loading_per_insured4,
              loading_per_insured5,
              loading_per_insured6,
              loading_per_insured7,
              loading_per_insured8,
              loading_per_insured9,
              loading_per_insured10,
              loading_per_insured11,
              loading_per_insured12,
              insured1_information2_1,
              insured1_information2_2,
              insured1_information2_3,
              insured1_information2_4,
              insured1_information2_5,
              insured1_information2_6,
              insured1_information2_7,
              insured1_information2_8,
              insured1_information2_9,
              insured1_information2_10,
              insured1_information2_11,
              insured1_information2_12               
            FROM
                ins.idst_renewal_data_rgs
            WHERE
                certificate_no = @PolicyNo";
            try
            {
                using (var connection = new NpgsqlConnection(connectionString))
                {
                    var result = await connection.QueryAsync<IdstData>(sqlQuery, new { PolicyNo = policyNo }).ConfigureAwait(false);
                    return result;
                }
            }
            catch (Exception ex)
            {
                // Log the error (use your preferred logging mechanism)
                Log.Error(ex, "An error occurred while fetching renewal data for policy: {PolicyNo}", policyNo);

                // Optionally, handle the error here or return a default value like an empty list
                return Enumerable.Empty<IdstData>();  // Returning an empty list in case of failure
            }
        }
        private async Task<IEnumerable<OptimaSecureRNE>> CalculateOptimaSecurePremiumqUpsell( IEnumerable<OptimaSecureRNE> osRNEData, string policyNo, Dictionary<string, Hashtable> baseRateHashTable, Dictionary<string, Hashtable> relations, Dictionary<string, Hashtable> cirates, Dictionary<string, Hashtable> deductableDiscount, Dictionary<string, Hashtable> hdcproportionsplit, Dictionary<string, Hashtable> hdcrates)
        {
            OptimaSecureRNE os = null;

            var columnNames = new List<string>();
            IEnumerable<IdstData> idstData = await GetIdstRenewalData(policyNo);
            var finalPremiumValues = new List<decimal?>();

            decimal? baseCrosscheck = 0;
            decimal? upsellCrosscheck = 0;
            List<decimal?> basesumInsuredList = new List<decimal?>();
            List<decimal?> upsellsumInsuredList = new List<decimal?>();
            foreach (var row in osRNEData)
            {
                var policNo16 = row.policy_number;
                var iDSTData = idstData.FirstOrDefault(x => x.certificate_no == policNo16);
                // Initialize DataTable and define columns
                DataTable table = new DataTable();
                // Define columns based on your headers
                for (int i = 11; i <= 1010; i += 1)  // Adjust the range if necessary
                {
                    table.Columns.Add($"covername{i}", typeof(string));
                    table.Columns.Add($"coversi{i}", typeof(string));
                    table.Columns.Add($"coverprem{i}", typeof(string));
                    table.Columns.Add($"coverloadingrate{i}", typeof(string));
                }
                // Add current data as a new row
                var data = ExtractData(row);
                DataRow newRow = table.NewRow();
                foreach (var column in data)
                {
                    // Check if the DataTable contains the column before setting its value
                    if (table.Columns.Contains(column.Key))
                    {
                        newRow[column.Key] = column.Value;
                    }
                }   // Add the populated DataRow to the DataTable
                table.Rows.Add(newRow);

                // Search for specific rider details
                string searchRider1 = "my:health Critical illness Add on";
                string searchRider2 = "my:health Hospital Cash Benefit Add On";
                string searchRider3 = "Unlimited Restore";

                DataTable siRiderOneDataTable = new DataTable();
                siRiderOneDataTable.Columns.Add("RiderName", typeof(string));
                siRiderOneDataTable.Columns.Add("SIValue", typeof(object));
                siRiderOneDataTable = GetRiderSI(table, searchRider1);

                DataTable siRiderTwoDataTable = new DataTable();
                siRiderTwoDataTable.Columns.Add("RiderName", typeof(string));
                siRiderTwoDataTable.Columns.Add("SIValue", typeof(object));
                // Retrieve and print Rider SI values based on Rider Name               
                siRiderTwoDataTable = GetRiderSI(table, searchRider2);


                DataTable siRiderThreeDataTable = new DataTable();
                siRiderThreeDataTable.Columns.Add("RiderName", typeof(string));
                siRiderThreeDataTable.Columns.Add("SIValue", typeof(object));
                // Retrieve and print Rider SI values based on Rider Name               
                siRiderThreeDataTable = GetRiderSI(table, searchRider3);

                string? policyLdDesc1 = row.pollddesc1;
                string? policyLdDesc2 = row.pollddesc2;
                string? policyLdDesc3 = row.pollddesc3;
                string? policyLdDesc4 = row.pollddesc4;
                string? policyLdDesc5 = row.pollddesc5;

                List<string?> policyLdDescValues = new List<string?>();
                policyLdDescValues.Add(policyLdDesc1);
                policyLdDescValues.Add(policyLdDesc2);
                policyLdDescValues.Add(policyLdDesc3);
                policyLdDescValues.Add(policyLdDesc4);
                policyLdDescValues.Add(policyLdDesc5);

                string? upsellType1 = row.upselltype1;
                string? upsellType2 = row.upselltype2;
                string? upsellType3 = row.upselltype3;
                string? upsellType4 = row.upselltype4;
                string? upsellType5 = row.upselltype5;

                List<string?> upselltypeValues = new List<string?>()
                        {
                            upsellType1,
                            upsellType2,
                            upsellType3,
                            upsellType4,
                            upsellType5
                        };

                string? upsellValue1 = row.upsellvalue1;
                string? upsellValue2 = row.upsellvalue2;
                string? upsellValue3 = row.upsellvalue3;
                string? upsellValue4 = row.upsellvalue4;
                string? upsellValue5 = row.upsellvalue5;

                List<string?> upsellvalueValues = new List<string?>()
                        {
                            upsellValue1,
                            upsellValue2,
                            upsellValue3,
                            upsellValue4,
                            upsellValue5
                        };


                List<int?> insuredAges = new List<int?>
                    {
                        TryParseAge(row.txt_insured_age1),
                        TryParseAge(row.txt_insured_age2),
                        TryParseAge(row.txt_insured_age3),
                        TryParseAge(row.txt_insured_age4),
                        TryParseAge(row.txt_insured_age5),
                        TryParseAge(row.txt_insured_age6),
                        TryParseAge(row.txt_insured_age7),
                        TryParseAge(row.txt_insured_age8),
                        TryParseAge(row.txt_insured_age9),
                        TryParseAge(row.txt_insured_age10),
                        TryParseAge(row.txt_insured_age11),
                        TryParseAge(row.txt_insured_age12)
                    };


                List<int?> ageValues = new List<int?>();
                void AddAge(string ageStr)
                {
                    if (int.TryParse(ageStr, out int age) && age != 0)
                    {
                        ageValues.Add(age);
                    }
                    else
                    {
                        ageValues.Add(null);
                    }
                }

                AddAge(row.txt_insured_age1);
                AddAge(row.txt_insured_age2);
                AddAge(row.txt_insured_age3);
                AddAge(row.txt_insured_age4);
                AddAge(row.txt_insured_age5);
                AddAge(row.txt_insured_age6);
                AddAge(row.txt_insured_age7);
                AddAge(row.txt_insured_age8);
                AddAge(row.txt_insured_age9);
                AddAge(row.txt_insured_age10);
                AddAge(row.txt_insured_age11);
                AddAge(row.txt_insured_age12);

                var nonNullAges = ageValues.Where(age => age.HasValue && age.Value != 0).ToList();
                var noOfMembers = nonNullAges.Count();
                var eldestMember = ageValues.Max();
                var numberOfMembers = noOfMembers;//calculate this field
                int? count = noOfMembers;


                //calculation of baseprem and crosscheck1 based on suminsured
                if ((row.sum_insured1.HasValue && row.sum_insured1 != null) || (row.sum_insured2.HasValue && row.sum_insured2 != null) || (row.sum_insured3.HasValue && row.sum_insured4 != null) || (row.sum_insured5.HasValue && row.sum_insured5 != null) || (row.sum_insured6.HasValue && row.sum_insured6 != null))
                {
                    for (int i = 1; i <= noOfMembers; i++)
                    {
                        decimal? sumInsured = (decimal?)row.GetType().GetProperty($"sum_insured{i}").GetValue(row);
                        basesumInsuredList.Add(sumInsured);
                    }

                    decimal? totalbasesuminsured = basesumInsuredList.Sum(si => si ?? 0);

                    string searchOnlineDescText = "ONLINE_DISCOUNT";
                    bool containsOnlineDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchOnlineDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultSearchOnlineDescText = containsOnlineDescText ? 1 : 0;

                    string searchDeductibleDescText = "DEDUCTIBLE_DISCOUNT";
                    bool containsdeuctibleDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchDeductibleDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultSearchdeuctibleDescText = containsdeuctibleDescText ? 1 : 0;

                    string searcFamilyDescText = "FAMILY_DISCOUNT";
                    bool containsFamilyDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searcFamilyDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultSearchFamilyDescText = containsFamilyDescText ? 1 : 0;

                    string searchEmployeeDescText = "EMPLOYEE_DISCOUNT";
                    bool containsEmployeeDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchEmployeeDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultSearchEmployeeDescText = containsEmployeeDescText ? 1 : 0;

                    string searchLoyaltyDescText = "LOYALTY_DISCOUNT";
                    bool containsLoyaltyDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchLoyaltyDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultLoyaltyDescText = containsLoyaltyDescText ? 1 : 0;

                    string searchCombiDescText = "COMBI_DISCOUNT";
                    bool containsCombiDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchCombiDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultCombiDescText = containsCombiDescText ? 1 : 0;

                    decimal? deductablesInsured1 = row.insured_deductable1;//c99
                    decimal? loyaltyDiscount = resultLoyaltyDescText;// row.loyalty_discount;//c798
                    decimal? employeeDiscount = resultSearchEmployeeDescText;//c799
                    decimal? onlineDiscount = resultSearchOnlineDescText;//c800


                    decimal? combiDiscounts = resultCombiDescText;//C803
                    var policyType = row.policy_type;

                    var policyperiod = row.policy_period;//c14

                    List<string> insuredRelations = new List<string>();

                    // Loop through the relation fields (1 to 12) and add them to the list
                    for (int i = 1; i <= 12; i++)
                    {
                        // Dynamically access the properties and add them to the list
                        var insuredRelation = row.GetType().GetProperty($"txt_insured_relation{i}")?.GetValue(row)?.ToString();
                        insuredRelations.Add(insuredRelation);
                    }


                    List<decimal?> cumulativeBonusList = new List<decimal?>();

                    for (int i = 1; i <= noOfMembers; i++)
                    {
                        decimal? bonusValue = Convert.ToDecimal(row.GetType().GetProperty($"insured_cb{i}")?.GetValue(row));
                        cumulativeBonusList.Add(bonusValue);
                    }

                    decimal? cumulativeBonus = cumulativeBonusList.Sum(cb => cb ?? 0);
                    var c15 = row.tier_type;       //c15
                    var c16 = row.policyplan;//c16

                    List<decimal> basicLoadingRates = new List<decimal>();

                    for (int i = 1; i <= noOfMembers; i++)
                    {
                        decimal? loadingRate = (decimal?)iDSTData.GetType().GetProperty($"loading_per_insured{i}")?.GetValue(iDSTData);

                        basicLoadingRates.Add(loadingRate ?? 0);
                    }

                    var deductibleDiscountVal = deductableDiscount
                             .Where(roww =>
                                 roww.Value is Hashtable rateDetails &&
                                 rateDetails["si"] != null && Convert.ToDecimal(rateDetails["si"]) == basesumInsuredList[0] &&
                                 rateDetails["deductible"] != null && Convert.ToDecimal(rateDetails["deductible"]) == deductablesInsured1
                             )
                             .Select(roww =>
                                 roww.Value is Hashtable details && details["discount"] != null
                                 ? Convert.ToDecimal(details["discount"])
                                 : (decimal?)null
                             )
                             .FirstOrDefault();
                    decimal? deductibleDiscount = deductibleDiscountVal ?? 0;
                    decimal? loyaltyDiscountValue = loyaltyDiscount;
                    loyaltyDiscountValue = loyaltyDiscount.HasValue && loyaltyDiscount.Value > 0 ? 2.5m : 0.0m;

                    // Get the value from the 799th column (index 799)
                    decimal? employeeDiscountValue = employeeDiscount;
                    employeeDiscountValue = employeeDiscount.HasValue && employeeDiscount.Value > 0 ? 5.0m : 0.0m;

                    decimal? onlineDiscountValue = onlineDiscount;
                    onlineDiscountValue = onlineDiscount.HasValue && onlineDiscount.Value > 0 ? 5.0m : 0.0m;

                    // Calculate the discount based on the policy type and number of members
                    decimal? familyDiscountValue = CalculateFamilyDiscount(policyType, numberOfMembers);
                    decimal? combiDiscountValue = CalculateCombiDiscount(combiDiscounts);

                    //// Calculate the percentage based on the policy period
                    decimal tenureDiscount = GetPolicyPercentage(policyperiod);
                    var columnName = GetColumnNameForPolicyPeriod(policyperiod);
                    if (columnName == null)
                    {
                        throw new ArgumentException($"Invalid policy period: {policyperiod}");
                    }
                    // Construct the raw SQL query
                    var sql = $@"
                SELECT {columnName}
                FROM baserate
                WHERE si = @p0 AND age = @p1 AND tier = @p2 AND product = @p3";
                    List<decimal?> basePremiumsList = new List<decimal?>();
                    decimal? basePrem = 0;

                    for (int i = 0; i < noOfMembers; i++)
                    {
                        basePrem = baseRateHashTable
                            .Where(row =>
                                row.Value is Hashtable rateDetails &&
                                (int)rateDetails["si"] == basesumInsuredList[i] &&  // Using sumInsuredList[i]
                                (int)rateDetails["age"] == insuredAges[i] &&
                                rateDetails["tier"].ToString() == c15 &&
                                rateDetails["product"].ToString() == c16)
                            .Select(row =>
                                row.Value is Hashtable details &&
                                details[columnName] != null
                                    ? Convert.ToDecimal(details[columnName])
                                    : (decimal?)null)
                            .FirstOrDefault();

                        basePremiumsList.Add(basePrem);
                    }
                    string condition = policyType; // Change to "INDIVIDUAL" to test the other case

                    decimal? basePremium = CalculateResult(condition, basePremiumsList);
                    deductibleDiscount = deductibleDiscount / 100;
                    var resultPremium = basePremium * deductibleDiscount;
                    decimal? basePremiumAfterDeductible = basePremium - resultPremium;


                    List<decimal?> loadingPremList = new List<decimal?>();
                    for (int i = 0; i < noOfMembers; i++)
                    {
                        decimal? basePre = basePremiumsList[i];
                        decimal? loadingRate = basicLoadingRates[i];
                        decimal? loadingPremm = CalculateLoadingPrem(basePre, loadingRate / 100);
                        loadingPremList.Add(loadingPremm);
                    }
                    decimal? loadingPrem = loadingPremList.Sum();

                    decimal? BaseAndLoading = basePremiumAfterDeductible + loadingPrem;
                    decimal? BaseAndLoadingLoyaltyDiscount = loyaltyDiscountValue / 100 * BaseAndLoading;
                    decimal? BaseAndLoadingEmployeeDiscount = employeeDiscountValue / 100 * BaseAndLoading;
                    decimal? BaseAndLoadingOnlineDiscount = (onlineDiscountValue / 100) * BaseAndLoading;
                    decimal? BaseAndLoadingFamilyDiscount = familyDiscountValue * BaseAndLoading;

                    decimal?[] cappedDiscountValues = new decimal?[]
                      {
                            BaseAndLoadingLoyaltyDiscount, // Value from cell C158
                            BaseAndLoadingEmployeeDiscount, // Value from cell C159
                            BaseAndLoadingOnlineDiscount, // Value from cell C160
                            BaseAndLoadingFamilyDiscount  // Value from cell C161
                      };

                    decimal? cappedDiscount = CalculateCappedDiscount(cappedDiscountValues, BaseAndLoading);
                    decimal? combiDiscount = (BaseAndLoading - cappedDiscount) * combiDiscountValue;
                    decimal? longTermDiscount = CalculatelongTermDiscount(BaseAndLoading, cappedDiscount, combiDiscount, tenureDiscount);
                    decimal? oSBasePremium = BaseAndLoading - cappedDiscount - combiDiscount - longTermDiscount;
                    oSBasePremium = oSBasePremium.HasValue ? Math.Round(oSBasePremium.Value, 2) : (decimal?)null;


                    decimal? unlimitedRestoreValue = 0;
                    if (siRiderThreeDataTable.Rows.Count >= 1)
                    {
                        foreach (DataRow itemRow in siRiderThreeDataTable.Rows)

                            unlimitedRestoreValue = 1;
                    }

                    // Apply the conditional logic
                    decimal? unlimitedRestore = unlimitedRestoreValue > 0 ? oSBasePremium * 0.005m : 0;//calculation required
                    decimal? finalBasePremium = oSBasePremium + unlimitedRestore;
                    decimal? SI = 0;
                    string Opt = "N";
                    if (siRiderOneDataTable.Rows.Count >= 1)
                    {
                        foreach (DataRow itemRow in siRiderOneDataTable.Rows)
                        {
                            Opt = "Y";
                            object siValueObject = itemRow["SIValue"];
                            SI = Convert.ToDecimal(siValueObject);
                        }
                    }
                    //var CIVariant1 = string.IsNullOrEmpty(iDSTData.insured1_information2_1) == null ? "9" : iDSTData.insured1_information2_1;
                    //var CIVariant2 = string.IsNullOrEmpty(iDSTData.insured1_information2_2) == null ? "9" : iDSTData.insured1_information2_2;
                    //var CIVariant3 = string.IsNullOrEmpty(iDSTData.insured1_information2_3) == null ? "9" : iDSTData.insured1_information2_3;
                    //var CIVariant4 = string.IsNullOrEmpty(iDSTData.insured1_information2_4) == null ? "9" : iDSTData.insured1_information2_4;
                    //var CIVariant5 = string.IsNullOrEmpty(iDSTData.insured1_information2_5) == null ? "9" : iDSTData.insured1_information2_5;
                    //var CIVariant6 = string.IsNullOrEmpty(iDSTData.insured1_information2_6) == null ? "9" : iDSTData.insured1_information2_6;
                    //var CIVariant7 = string.IsNullOrEmpty(iDSTData.insured1_information2_7) == null ? "9" : iDSTData.insured1_information2_7;
                    //var CIVariant8 = string.IsNullOrEmpty(iDSTData.insured1_information2_8) == null ? "9" : iDSTData.insured1_information2_8;
                    //var CIVariant9 = string.IsNullOrEmpty(iDSTData.insured1_information2_9) == null ? "9" : iDSTData.insured1_information2_9;
                    //var CIVariant10 = string.IsNullOrEmpty(iDSTData.insured1_information2_10) == null? "9" : iDSTData.insured1_information2_10;
                    //var CIVariant11 = string.IsNullOrEmpty(iDSTData.insured1_information2_11) == null ? "9" : iDSTData.insured1_information2_11;
                    //var CIVariant12 = string.IsNullOrEmpty(iDSTData.insured1_information2_12) == null ? "9" : iDSTData.insured1_information2_12;
                    List<decimal> ciVariants = new List<decimal>();
                    for (int i = 1; i <= noOfMembers; i++)
                    {
                        decimal? ciVariant = (decimal?)iDSTData.GetType().GetProperty($"insured1_information2_{i}")?.GetValue(iDSTData);

                        ciVariants.Add(ciVariant ?? 9);
                    }
                    // decimal? CIVariant = SI != 0 ? 9 : 0;
                    var policyPeriod = GetColumnNameForPolicyPeriod(policyperiod);
                    if (policyPeriod == null)
                    {
                        throw new ArgumentException($"Invalid policy period: {policyperiod}");
                    }
                    var sqlpolicyPeriod = $@"
                SELECT {policyPeriod}
                FROM cirates
                WHERE age = @p0 AND ci_variant = @p1";
                    List<decimal?> ciRates = new List<decimal?>();
                    for (int i = 0; i < insuredAges.Count; i++)
                    {
                        if (insuredAges[i].HasValue)  // Check if the age is valid
                        {
                            decimal? ciRate = await GetCIRate(insuredAges[i].Value, ciVariants[i], policyPeriod, sqlpolicyPeriod, cirates);
                            ciRates.Add(ciRate);  // Add the resulting CI rate to the list
                        }
                        else
                        {
                            ciRates.Add(null);  // If the age is invalid, add a null CI rate
                        }
                    }
                    List<decimal?> premiums = new List<decimal?>();
                    foreach (var ciRate in ciRates)
                    {
                        decimal? processedCiRate = GetCiRatesValues(SI, ciRate);  // Process the CI rate using the existing method
                        premiums.Add(processedCiRate);  // Add the processed rate to the list
                    }
                    decimal? premium = premiums.Sum();
                    List<decimal?> loadingPremiumvalues = new List<decimal?>();
                    for (int i = 1; i < noOfMembers; i++)
                    {
                        decimal? premiumm = premiums[i]; // Fetch the premium for the current member
                        decimal? basicLoadingRate = basicLoadingRates[i]; // Fetch the loading rate for the current member
                        decimal? loadingPremm = CalculateResultMyBaseLoading(premium, basicLoadingRate);

                        // Store the result in the list
                        loadingPremiumvalues.Add(loadingPrem);
                    }
                    decimal? loadingPremium = loadingPremiumvalues.Sum();
                    decimal? cIloyaltyDiscountValue;
                    decimal? cIloyaltyDiscount;
                    GetLoyaltyDiscount(loyaltyDiscountValue, out cIloyaltyDiscountValue, out cIloyaltyDiscount);
                    //GetLoyaltyDiscount(loyaltyDiscount, out cIloyaltyDiscountValue, out cIloyaltyDiscount);
                    // Get the value from the 799th column (index 799)
                    decimal? cIemployeeDiscountValue = employeeDiscount;
                    decimal? cIonlineDiscountValue = onlineDiscount;
                    decimal? cIFamilyDiscountValue = GetFamilyDiscount(noOfMembers);

                    decimal? cIBaseAndLoading = premium + loadingPremium;
                    decimal? cIBaseLoyaltyDisocunt = cIloyaltyDiscount * cIBaseAndLoading;
                    decimal? cIBaseEmployeeDisocunt = employeeDiscountValue / 100 * cIBaseAndLoading;
                    decimal? cIBaseOnlineDisocunt = onlineDiscountValue / 100 * cIBaseAndLoading;
                    decimal? cIBaseFamilyDisocunt = cIBaseAndLoading * cIFamilyDiscountValue;
                    decimal?[] cIcappedDiscountValues = new decimal?[]
                    {
                            cIBaseLoyaltyDisocunt, // Value from cell C158
                            cIBaseEmployeeDisocunt, // Value from cell C159
                            cIBaseOnlineDisocunt, // Value from cell C160
                            cIBaseFamilyDisocunt  // Value from cell C161
                    };
                    decimal? cicappedDiscount = CalculateCappedDiscount(cappedDiscountValues, cIBaseAndLoading);
                    decimal?[] cicappedDiscountValues = new decimal?[]
                      {
                            cIBaseLoyaltyDisocunt, // Value from cell C158
                            cIBaseEmployeeDisocunt, // Value from cell C159
                            cIBaseOnlineDisocunt, // Value from cell C160
                            cIBaseFamilyDisocunt  // Value from cell C161
                      };
                    decimal? cIBasecappedDiscount = CalculateCappedDiscount(cicappedDiscountValues, cIBaseAndLoading);
                    decimal? CIBaselongTermDiscount = (cIBaseAndLoading - cIBasecappedDiscount) * tenureDiscount;

                    decimal? cIBaseBaseCoverPremium = cIBaseAndLoading - cIBasecappedDiscount - CIBaselongTermDiscount;

                    string cashBenefitOpt = "N";
                    decimal? hdcsi = 0;
                    if (siRiderTwoDataTable.Rows.Count >= 1)
                    {
                        foreach (DataRow itemRow in siRiderTwoDataTable.Rows)
                        {
                            cashBenefitOpt = "Y";
                            object hdcsiValueObject = itemRow["SIValue"];
                            hdcsi = Convert.ToDecimal(hdcsiValueObject);
                        }
                    }

                    var insuredRelationTAGValues = new List<string?>();
                    for (int i = 0; i < insuredRelations.Count; i++)
                    {
                        var insuredRelation = insuredRelations[i];
                        var insuredRelationTAG = relations
                            .Where(roww =>
                                roww.Value is Hashtable rateDetails &&
                                rateDetails["insured_relation"]?.ToString() == insuredRelation)
                            .Select(roww =>
                                roww.Value is Hashtable details && details["relation_tag"] != null
                                ? Convert.ToString(details["relation_tag"])
                                : (string?)null)
                            .FirstOrDefault();

                        insuredRelationTAGValues.Add(insuredRelationTAG);
                    }
                    // Perform the count and apply the logic
                    string aValue = ProcessCountForA(insuredRelationTAGValues);
                    string pValue = ProcessCountForP(insuredRelationTAGValues);
                    string cValue = ProcessCountForC(insuredRelationTAGValues);
                    string A = aValue;

                    var hdcAgeBand = hdcrates
                       .Where(roww =>
                           roww.Value is Hashtable rateDetails &&
                           rateDetails.ContainsKey("age") && rateDetails["age"] != null && (int)rateDetails["age"] == eldestMember &&
                           rateDetails.ContainsKey("plan_type") && rateDetails["plan_type"] != null && rateDetails["plan_type"].ToString() == A.ToString()
                       )
                       .Select(roww =>
                           roww.Value is Hashtable details &&
                           details.ContainsKey("age_band") && details["age_band"] != null
                           ? details["age_band"].ToString()
                           : null
                       )
                      .FirstOrDefault();

                    string? P = pValue;
                    string? C = cValue;
                    string? familyDefn = (A ?? string.Empty) + (P ?? string.Empty) + (C ?? string.Empty);
                    familyDefn = familyDefn.Trim();

                    // Construct the raw SQL query
                    var sqlperiod = $@"
                SELECT {columnName}
                FROM hdcrates
                WHERE si = @p0 AND age = @p1 AND age_band = @p2 AND plan_type=@p3";

                    // Execute the raw SQL query
                    var cashBenefitPremiumValue = hdcrates
                       .Where(roww =>
                           roww.Value is Hashtable rateDetails &&
                           rateDetails["si"] != null && (rateDetails["si"] as int? ?? 0) == hdcsi &&
                           rateDetails["age"] != null && (rateDetails["age"] as int? ?? 0) == eldestMember &&
                           rateDetails["age_band"] != null && rateDetails["age_band"].ToString() == hdcAgeBand &&
                           rateDetails["plan_type"] != null && rateDetails["plan_type"].ToString() == familyDefn
                       )
                       .Select(roww =>
                           roww.Value is Hashtable details && details[columnName] != null
                           ? Convert.ToDecimal(details[columnName])
                           : (decimal?)null
                       )
                       .FirstOrDefault();
                    decimal? cashBenefitSI = hdcsi;

                    string? ageBand = hdcAgeBand;
                    cashBenefitPremiumValue = cashBenefitPremiumValue ?? 0m;
                    decimal? cashBenefitPremium = cashBenefitPremiumValue;
                    // Construct the raw SQL query

                    var dictionary = new Dictionary<string, List<string>>
                {
                    { "2a", GenerateColumnNames("2a") },
                     { "2c", GenerateColumnNames("2c")},
                      { "2a2p", Generate2A2PColumnNames("2a2p") },
                      { "2a2c", Generate2A2PColumnNames("2a2c") }
                };

                    var key = familyDefn.ToLower();
                    var columnNamesString = "";
                    if (dictionary.TryGetValue(key, out var columnValues))
                    {
                        // Convert the list of column names to a single string
                        columnNamesString = string.Join(", ", columnValues);
                        Console.WriteLine($"Key: {key}");
                        Console.WriteLine($"Column Values: {columnNamesString}");
                    }
                    ////{columnNamesString},p1, c1, c2, c3, p2,eldest_member_age_band, family_composition
                    var sqlhdcproportionsplit = $@"
                SELECT * 
                FROM hdcproportionsplit
                WHERE  eldest_member_age_band = @p0 AND family_composition = @p1";
                    //Execute the raw SQL query

                    var results = hdcproportionsplit
                             .Where(roww =>
                             {
                                 if (roww.Value is Hashtable rateDetails)
                                 {
                                     // Check that "eldest_member_age_band" exists and is not null
                                     var eldestMemberAgeBand = rateDetails["eldest_member_age_band"];
                                     var deductible = rateDetails["family_composition"];

                                     return eldestMemberAgeBand != null &&
                                            eldestMemberAgeBand.ToString() == hdcAgeBand && // Correct comparison for "eldest_member_age_band"
                                            deductible != null &&
                                            deductible.ToString() == familyDefn; // Correct comparison for "deductible"
                                 }
                                 return false;
                             })
                         .Select(roww =>
                         {
                             if (roww.Value is Hashtable details)
                             {
                                 // Only return a result if the necessary keys exist
                                 return new
                                 {
                                     eldest_member_age_band = details["eldest_member_age_band"]?.ToString(), // Use null-conditional operator
                                     family_composition = details["family_composition"]?.ToString(),
                                     a1 = details["a1"]?.ToString(),
                                     a2 = details["a2"]?.ToString(),
                                     p1 = details["p1"]?.ToString(),
                                     p2 = details["p2"]?.ToString(),
                                     c1 = details["c1"]?.ToString(),
                                     c2 = details["c2"]?.ToString()
                                 };
                             }
                             return null; // Return null if `details` is not a Hashtable
                         })
                         .Where(details => details != null) // Filter out null results
                         .ToList(); // Get the results as a list
                    var selectedValues = results.Select(r => new
                    {
                        a1 = Convert.ToDecimal(r.a1),
                        a2 = Convert.ToDecimal(r.a2),
                        p1 = Convert.ToDecimal(r.p1),
                        p2 = Convert.ToDecimal(r.p2),
                        c1 = Convert.ToDecimal(r.c1),
                        c2 = Convert.ToDecimal(r.c2)
                    }).FirstOrDefault();


                    decimal? Insured_1 = selectedValues?.a1 * cashBenefitPremiumValue;
                    decimal? Insured_2 = selectedValues?.a2 * cashBenefitPremiumValue;
                    decimal? Insured_3 = 0m;
                    decimal? Insured_4 = 0m;
                    if (familyDefn == "2A 2P")
                    {
                        Insured_3 = selectedValues?.p1 * cashBenefitPremiumValue;
                        Insured_4 = selectedValues?.p2 * cashBenefitPremiumValue;
                    }
                    if (familyDefn == "2A 2C")
                    {
                        Insured_3 = selectedValues?.c1 * cashBenefitPremiumValue;
                        Insured_4 = (selectedValues?.c2) * cashBenefitPremiumValue;
                    }

                    decimal? Insured_5 = 0;
                    decimal? Insured_6 = 0;
                    decimal? Insured_7 = 0;
                    decimal? Insured_8 = 0;
                    decimal? Insured_9 = 0;
                    decimal? Insured_10 = 0;
                    decimal? Insured_11 = 0;
                    decimal? Insured_12 = 0;


                    List<decimal?> premiumCheckInsuredValues = new List<decimal?>();
                    premiumCheckInsuredValues.Add(Insured_1);
                    premiumCheckInsuredValues.Add(Insured_2);
                    premiumCheckInsuredValues.Add(Insured_3);
                    premiumCheckInsuredValues.Add(Insured_4);
                    premiumCheckInsuredValues.Add(Insured_5);
                    premiumCheckInsuredValues.Add(Insured_6);
                    premiumCheckInsuredValues.Add(Insured_7);
                    premiumCheckInsuredValues.Add(Insured_8);
                    premiumCheckInsuredValues.Add(Insured_9);
                    premiumCheckInsuredValues.Add(Insured_10);
                    premiumCheckInsuredValues.Add(Insured_11);
                    premiumCheckInsuredValues.Add(Insured_12);

                    decimal? cashBenefitLoadingPremiumSum = premiumCheckInsuredValues.Sum();
                    decimal? premiumCheck = cashBenefitLoadingPremiumSum - cashBenefitPremiumValue;

                    // Calculate the result and handle errors              
                    List<decimal?> cashBenefitLoadingPremiumValues = new List<decimal?>();
                    for (int i = 0; i < noOfMembers; i++)
                    {
                        decimal? insuredValue = premiumCheckInsuredValues[i]; // Fetch the insured value for the current member
                        decimal? basicLoadingRate = basicLoadingRates[i]; // Fetch the loading rate for the current member

                        decimal? cashBenefit = CalculateLoadingPremium(insuredValue, basicLoadingRate / 100);
                        cashBenefitLoadingPremiumValues.Add(cashBenefit);
                    }

                    decimal? cashBenefitLoadingPremium = cashBenefitLoadingPremiumValues.Sum();

                    decimal? hDCBaseAndLoading = cashBenefitPremium + cashBenefitLoadingPremium;
                    decimal? hDCBaseAndLoadingLoyaltyDiscount = hDCBaseAndLoading * cIloyaltyDiscount;
                    decimal? hDCBaseAndLoadingEmployeeDiscount = hDCBaseAndLoading * GetEmployeeDiscount(employeeDiscountValue);
                    onlineDiscountValue = onlineDiscountValue / 100;
                    decimal? hDCOnlineDisocuntValue = hDCBaseAndLoading * (onlineDiscountValue);//GetOnlineDiscount(noOfMembers);
                    decimal? hDCBaseAndLoadingOnlineDiscount = hDCOnlineDisocuntValue;
                    decimal? hDCBaseAndLoadingFamilyDiscount = hDCBaseAndLoading * (familyDiscountValue);
                    decimal?[] hDCcappedDiscountValues = new decimal?[]
                     {
                            hDCBaseAndLoadingLoyaltyDiscount, // Value from cell C158
                            hDCBaseAndLoadingEmployeeDiscount, // Value from cell C159
                            hDCBaseAndLoadingOnlineDiscount, // Value from cell C160
                            hDCBaseAndLoadingFamilyDiscount  // Value from cell C161
                     };

                    decimal? hDCCappedDiscount = CalculateCappedDiscount(hDCcappedDiscountValues, hDCBaseAndLoading);

                    decimal? hDClongTermDiscount = (hDCBaseAndLoading - hDCCappedDiscount) * tenureDiscount;

                    decimal? hDCBaseCoverPremium = hDCBaseAndLoading - hDCCappedDiscount - hDClongTermDiscount;

                    decimal? netPremium = (finalBasePremium + cIBaseBaseCoverPremium + hDCBaseCoverPremium);

                    decimal? GST = netPremium * 0.18m;

                    decimal? finalPremium = netPremium + GST;

                    baseCrosscheck = row.num_tot_premium - finalPremium;
                }

                //calculation of upsellbaseprem and crosscheck2 based on upsell value
                if (row.upselltype1 != null || row.upselltype2 != null || row.upselltype3 != null || row.upselltype4 != null || row.upselltype5 != null)
                {
                    for (int i = 1; i <= noOfMembers; i++)
                    {
                        var upsellValueStr = row.GetType().GetProperty($"upsellvalue{i}")?.GetValue(row) as string;
                        decimal? sumInsured = null;
                        if (!string.IsNullOrEmpty(upsellValueStr) && decimal.TryParse(upsellValueStr, out decimal parsedValue))
                        {
                            sumInsured = parsedValue;
                        }
                        upsellsumInsuredList.Add(sumInsured);
                    }
                    string searchUpsellType1 = "SI_UPSELL";
                    string searchUpsellType2 = "UPSELLBASESI_1";
                    bool containsUpsellType = upselltypeValues.Any(upsell => upsell != null &&
                        (upsell.Contains(searchUpsellType1, StringComparison.OrdinalIgnoreCase) ||
                         upsell.Contains(searchUpsellType2, StringComparison.OrdinalIgnoreCase)));
                    if (containsUpsellType)
                    {
                        if (noOfMembers > 0)
                        {
                            if (upselltypeValues.Contains(searchUpsellType1, StringComparer.OrdinalIgnoreCase))
                            {
                                if (decimal.TryParse(upsellValue1, out decimal parsedValue1))
                                {
                                    for (int i = 1; i <= noOfMembers; i++)
                                    {
                                        upsellsumInsuredList[i - 1] = parsedValue1;
                                    }
                                }
                            }
                            else if (upselltypeValues.Contains(searchUpsellType2, StringComparer.OrdinalIgnoreCase))
                            {
                                if (decimal.TryParse(upsellValue1, out decimal parsedValue2))
                                {
                                    for (int i = 1; i <= noOfMembers; i++)
                                    {
                                        upsellsumInsuredList[i - 1] = parsedValue2;
                                    }
                                }
                            }
                            else
                            {
                                for (int i = 0; i <= noOfMembers; i++)
                                {
                                    if (decimal.TryParse(upsellValue1, out decimal parsedValue1))
                                    {
                                        upsellsumInsuredList[0] = parsedValue1;
                                    }
                                    if (noOfMembers > 1 && decimal.TryParse(upsellValue2, out decimal parsedValue2))
                                    {
                                        upsellsumInsuredList[1] = parsedValue2;
                                    }
                                    if (noOfMembers > 2 && decimal.TryParse(upsellValue3, out decimal parsedValue3))
                                    {
                                        upsellsumInsuredList[2] = parsedValue3;
                                    }
                                    if (noOfMembers > 3 && decimal.TryParse(upsellValue4, out decimal parsedValue4))
                                    {
                                        upsellsumInsuredList[3] = parsedValue4;
                                    }
                                    if (noOfMembers > 4 && decimal.TryParse(upsellValue5, out decimal parsedValue5))
                                    {
                                        upsellsumInsuredList[4] = parsedValue5;
                                    }
                                    if (noOfMembers > 5 && decimal.TryParse(upsellValue5, out decimal parsedValue6)) // Assuming same value for siSix
                                    {
                                        upsellsumInsuredList[5] = parsedValue6;
                                    }
                                }
                            }
                        }
                    };

                    decimal? totalupsellsuminsured = upsellsumInsuredList.Sum(si => si ?? 0);
                    string searchOnlineDescText = "ONLINE_DISCOUNT";
                    bool containsOnlineDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchOnlineDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultSearchOnlineDescText = containsOnlineDescText ? 1 : 0;

                    string searchDeductibleDescText = "DEDUCTIBLE_DISCOUNT";
                    bool containsdeuctibleDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchDeductibleDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultSearchdeuctibleDescText = containsdeuctibleDescText ? 1 : 0;

                    string searcFamilyDescText = "FAMILY_DISCOUNT";
                    bool containsFamilyDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searcFamilyDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultSearchFamilyDescText = containsFamilyDescText ? 1 : 0;

                    string searchEmployeeDescText = "EMPLOYEE_DISCOUNT";
                    bool containsEmployeeDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchEmployeeDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultSearchEmployeeDescText = containsEmployeeDescText ? 1 : 0;

                    string searchLoyaltyDescText = "LOYALTY_DISCOUNT";
                    bool containsLoyaltyDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchLoyaltyDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultLoyaltyDescText = containsLoyaltyDescText ? 1 : 0;

                    string searchCombiDescText = "COMBI_DISCOUNT";
                    bool containsCombiDescText = policyLdDescValues.Any(desc => desc != null && desc.Contains(searchCombiDescText, StringComparison.OrdinalIgnoreCase));
                    decimal? resultCombiDescText = containsCombiDescText ? 1 : 0;

                    decimal? deductablesInsured1 = row.insured_deductable1;//c99
                    decimal? loyaltyDiscount = resultLoyaltyDescText;// row.loyalty_discount;//c798
                    decimal? employeeDiscount = resultSearchEmployeeDescText;//c799
                    decimal? onlineDiscount = resultSearchOnlineDescText;//c800


                    decimal? combiDiscounts = resultCombiDescText;//C803
                    var policyType = row.policy_type;

                    var policyperiod = row.policy_period;//c14

                    List<string> insuredRelations = new List<string>();

                    // Loop through the relation fields (1 to 12) and add them to the list
                    for (int i = 1; i <= 12; i++)
                    {
                        // Dynamically access the properties and add them to the list
                        var insuredRelation = row.GetType().GetProperty($"txt_insured_relation{i}")?.GetValue(row)?.ToString();
                        insuredRelations.Add(insuredRelation);
                    }


                    List<decimal?> cumulativeBonusList = new List<decimal?>();

                    for (int i = 1; i <= noOfMembers; i++)
                    {
                        decimal? bonusValue = Convert.ToDecimal(row.GetType().GetProperty($"insured_cb{i}")?.GetValue(row));
                        cumulativeBonusList.Add(bonusValue);
                    }

                    decimal? cumulativeBonus = cumulativeBonusList.Sum(cb => cb ?? 0);
                    var c15 = row.tier_type;       //c15
                    var c16 = row.policyplan;//c16

                    List<decimal> basicLoadingRates = new List<decimal>();

                    for (int i = 1; i <= noOfMembers; i++)
                    {
                        decimal? loadingRate = (decimal?)iDSTData.GetType().GetProperty($"loading_per_insured{i}")?.GetValue(iDSTData);

                        basicLoadingRates.Add(loadingRate ?? 0);
                    }

                    var deductibleDiscountVal = deductableDiscount
                             .Where(roww =>
                                 roww.Value is Hashtable rateDetails &&
                                 rateDetails["si"] != null && Convert.ToDecimal(rateDetails["si"]) == upsellsumInsuredList[0] &&
                                 rateDetails["deductible"] != null && Convert.ToDecimal(rateDetails["deductible"]) == deductablesInsured1
                             )
                             .Select(roww =>
                                 roww.Value is Hashtable details && details["discount"] != null
                                 ? Convert.ToDecimal(details["discount"])
                                 : (decimal?)null
                             )
                             .FirstOrDefault();
                    decimal? deductibleDiscount = deductibleDiscountVal ?? 0;
                    decimal? loyaltyDiscountValue = loyaltyDiscount;
                    loyaltyDiscountValue = loyaltyDiscount.HasValue && loyaltyDiscount.Value > 0 ? 2.5m : 0.0m;

                    // Get the value from the 799th column (index 799)
                    decimal? employeeDiscountValue = employeeDiscount;
                    employeeDiscountValue = employeeDiscount.HasValue && employeeDiscount.Value > 0 ? 5.0m : 0.0m;

                    decimal? onlineDiscountValue = onlineDiscount;
                    onlineDiscountValue = onlineDiscount.HasValue && onlineDiscount.Value > 0 ? 5.0m : 0.0m;

                    // Calculate the discount based on the policy type and number of members
                    decimal? familyDiscountValue = CalculateFamilyDiscount(policyType, numberOfMembers);
                    decimal? combiDiscountValue = CalculateCombiDiscount(combiDiscounts);

                    //// Calculate the percentage based on the policy period
                    decimal tenureDiscount = GetPolicyPercentage(policyperiod);
                    var columnName = GetColumnNameForPolicyPeriod(policyperiod);
                    if (columnName == null)
                    {
                        throw new ArgumentException($"Invalid policy period: {policyperiod}");
                    }
                    // Construct the raw SQL query
                    var sql = $@"
 SELECT {columnName}
 FROM baserate
 WHERE si = @p0 AND age = @p1 AND tier = @p2 AND product = @p3";
                    List<decimal?> basePremiumsList = new List<decimal?>();
                    decimal? basePrem = 0;

                    for (int i = 0; i < noOfMembers; i++)
                    {
                        basePrem = baseRateHashTable
                            .Where(row =>
                                row.Value is Hashtable rateDetails &&
                                (int)rateDetails["si"] == upsellsumInsuredList[i] &&  // Using sumInsuredList[i]
                                (int)rateDetails["age"] == insuredAges[i] &&
                                rateDetails["tier"].ToString() == c15 &&
                                rateDetails["product"].ToString() == c16)
                            .Select(row =>
                                row.Value is Hashtable details &&
                                details[columnName] != null
                                    ? Convert.ToDecimal(details[columnName])
                                    : (decimal?)null)
                            .FirstOrDefault();

                        basePremiumsList.Add(basePrem);
                    }
                    string condition = policyType; // Change to "INDIVIDUAL" to test the other case

                    decimal? basePremium = CalculateResult(condition, basePremiumsList);
                    deductibleDiscount = deductibleDiscount / 100;
                    var resultPremium = basePremium * deductibleDiscount;
                    decimal? basePremiumAfterDeductible = basePremium - resultPremium;


                    List<decimal?> loadingPremList = new List<decimal?>();
                    for (int i = 0; i < noOfMembers; i++)
                    {
                        decimal? basePre = basePremiumsList[i];
                        decimal? loadingRate = basicLoadingRates[i];
                        decimal? loadingPremm = CalculateLoadingPrem(basePre, loadingRate / 100);
                        loadingPremList.Add(loadingPremm);
                    }
                    decimal? loadingPrem = loadingPremList.Sum();

                    decimal? BaseAndLoading = basePremiumAfterDeductible + loadingPrem;
                    decimal? BaseAndLoadingLoyaltyDiscount = loyaltyDiscountValue / 100 * BaseAndLoading;
                    decimal? BaseAndLoadingEmployeeDiscount = employeeDiscountValue / 100 * BaseAndLoading;
                    decimal? BaseAndLoadingOnlineDiscount = (onlineDiscountValue / 100) * BaseAndLoading;
                    decimal? BaseAndLoadingFamilyDiscount = familyDiscountValue * BaseAndLoading;

                    decimal?[] cappedDiscountValues = new decimal?[]
                      {
             BaseAndLoadingLoyaltyDiscount, // Value from cell C158
             BaseAndLoadingEmployeeDiscount, // Value from cell C159
             BaseAndLoadingOnlineDiscount, // Value from cell C160
             BaseAndLoadingFamilyDiscount  // Value from cell C161
                      };

                    decimal? cappedDiscount = CalculateCappedDiscount(cappedDiscountValues, BaseAndLoading);
                    decimal? combiDiscount = (BaseAndLoading - cappedDiscount) * combiDiscountValue;
                    decimal? longTermDiscount = CalculatelongTermDiscount(BaseAndLoading, cappedDiscount, combiDiscount, tenureDiscount);
                    decimal? oSBasePremium = BaseAndLoading - cappedDiscount - combiDiscount - longTermDiscount;
                    oSBasePremium = oSBasePremium.HasValue ? Math.Round(oSBasePremium.Value, 2) : (decimal?)null;


                    decimal? unlimitedRestoreValue = 0;
                    if (siRiderThreeDataTable.Rows.Count >= 1)
                    {
                        foreach (DataRow itemRow in siRiderThreeDataTable.Rows)

                            unlimitedRestoreValue = 1;
                    }

                    // Apply the conditional logic
                    decimal? unlimitedRestore = unlimitedRestoreValue > 0 ? oSBasePremium * 0.005m : 0;//calculation required
                    decimal? finalBasePremium = oSBasePremium + unlimitedRestore;
                    decimal? SI = 0;
                    string Opt = "N";
                    if (siRiderOneDataTable.Rows.Count >= 1)
                    {
                        foreach (DataRow itemRow in siRiderOneDataTable.Rows)
                        {
                            Opt = "Y";
                            object siValueObject = itemRow["SIValue"];
                            SI = Convert.ToDecimal(siValueObject);
                        }
                    }
                    //var CIVariant1 = string.IsNullOrEmpty(iDSTData.insured1_information2_1) == null ? "9" : iDSTData.insured1_information2_1;
                    //var CIVariant2 = string.IsNullOrEmpty(iDSTData.insured1_information2_2) == null ? "9" : iDSTData.insured1_information2_2;
                    //var CIVariant3 = string.IsNullOrEmpty(iDSTData.insured1_information2_3) == null ? "9" : iDSTData.insured1_information2_3;
                    //var CIVariant4 = string.IsNullOrEmpty(iDSTData.insured1_information2_4) == null ? "9" : iDSTData.insured1_information2_4;
                    //var CIVariant5 = string.IsNullOrEmpty(iDSTData.insured1_information2_5) == null ? "9" : iDSTData.insured1_information2_5;
                    //var CIVariant6 = string.IsNullOrEmpty(iDSTData.insured1_information2_6) == null ? "9" : iDSTData.insured1_information2_6;
                    //var CIVariant7 = string.IsNullOrEmpty(iDSTData.insured1_information2_7) == null ? "9" : iDSTData.insured1_information2_7;
                    //var CIVariant8 = string.IsNullOrEmpty(iDSTData.insured1_information2_8) == null ? "9" : iDSTData.insured1_information2_8;
                    //var CIVariant9 = string.IsNullOrEmpty(iDSTData.insured1_information2_9) == null ? "9" : iDSTData.insured1_information2_9;
                    //var CIVariant10 = string.IsNullOrEmpty(iDSTData.insured1_information2_10) == null? "9" : iDSTData.insured1_information2_10;
                    //var CIVariant11 = string.IsNullOrEmpty(iDSTData.insured1_information2_11) == null ? "9" : iDSTData.insured1_information2_11;
                    //var CIVariant12 = string.IsNullOrEmpty(iDSTData.insured1_information2_12) == null ? "9" : iDSTData.insured1_information2_12;
                    List<decimal> ciVariants = new List<decimal>();
                    for (int i = 1; i <= noOfMembers; i++)
                    {
                        decimal? ciVariant = (decimal?)iDSTData.GetType().GetProperty($"insured1_information2_{i}")?.GetValue(iDSTData);

                        ciVariants.Add(ciVariant ?? 9);
                    }
                    // decimal? CIVariant = SI != 0 ? 9 : 0;
                    var policyPeriod = GetColumnNameForPolicyPeriod(policyperiod);
                    if (policyPeriod == null)
                    {
                        throw new ArgumentException($"Invalid policy period: {policyperiod}");
                    }
                    var sqlpolicyPeriod = $@"
 SELECT {policyPeriod}
 FROM cirates
 WHERE age = @p0 AND ci_variant = @p1";
                    List<decimal?> ciRates = new List<decimal?>();
                    for (int i = 0; i < insuredAges.Count; i++)
                    {
                        if (insuredAges[i].HasValue)  // Check if the age is valid
                        {
                            decimal? ciRate = await GetCIRate(insuredAges[i].Value, ciVariants[i], policyPeriod, sqlpolicyPeriod, cirates);
                            ciRates.Add(ciRate);  // Add the resulting CI rate to the list
                        }
                        else
                        {
                            ciRates.Add(null);  // If the age is invalid, add a null CI rate
                        }
                    }
                    List<decimal?> premiums = new List<decimal?>();
                    foreach (var ciRate in ciRates)
                    {
                        decimal? processedCiRate = GetCiRatesValues(SI, ciRate);  // Process the CI rate using the existing method
                        premiums.Add(processedCiRate);  // Add the processed rate to the list
                    }
                    decimal? premium = premiums.Sum();
                    List<decimal?> loadingPremiumvalues = new List<decimal?>();
                    for (int i = 1; i < noOfMembers; i++)
                    {
                        decimal? premiumm = premiums[i]; // Fetch the premium for the current member
                        decimal? basicLoadingRate = basicLoadingRates[i]; // Fetch the loading rate for the current member
                        decimal? loadingPremm = CalculateResultMyBaseLoading(premium, basicLoadingRate);

                        // Store the result in the list
                        loadingPremiumvalues.Add(loadingPrem);
                    }
                    decimal? loadingPremium = loadingPremiumvalues.Sum();
                    decimal? cIloyaltyDiscountValue;
                    decimal? cIloyaltyDiscount;
                    GetLoyaltyDiscount(loyaltyDiscountValue, out cIloyaltyDiscountValue, out cIloyaltyDiscount);
                    //GetLoyaltyDiscount(loyaltyDiscount, out cIloyaltyDiscountValue, out cIloyaltyDiscount);
                    // Get the value from the 799th column (index 799)
                    decimal? cIemployeeDiscountValue = employeeDiscount;
                    decimal? cIonlineDiscountValue = onlineDiscount;
                    decimal? cIFamilyDiscountValue = GetFamilyDiscount(noOfMembers);

                    decimal? cIBaseAndLoading = premium + loadingPremium;
                    decimal? cIBaseLoyaltyDisocunt = cIloyaltyDiscount * cIBaseAndLoading;
                    decimal? cIBaseEmployeeDisocunt = employeeDiscountValue / 100 * cIBaseAndLoading;
                    decimal? cIBaseOnlineDisocunt = onlineDiscountValue / 100 * cIBaseAndLoading;
                    decimal? cIBaseFamilyDisocunt = cIBaseAndLoading * cIFamilyDiscountValue;
                    decimal?[] cIcappedDiscountValues = new decimal?[]
                    {
             cIBaseLoyaltyDisocunt, // Value from cell C158
             cIBaseEmployeeDisocunt, // Value from cell C159
             cIBaseOnlineDisocunt, // Value from cell C160
             cIBaseFamilyDisocunt  // Value from cell C161
                    };
                    decimal? cicappedDiscount = CalculateCappedDiscount(cappedDiscountValues, cIBaseAndLoading);
                    decimal?[] cicappedDiscountValues = new decimal?[]
                      {
             cIBaseLoyaltyDisocunt, // Value from cell C158
             cIBaseEmployeeDisocunt, // Value from cell C159
             cIBaseOnlineDisocunt, // Value from cell C160
             cIBaseFamilyDisocunt  // Value from cell C161
                      };
                    decimal? cIBasecappedDiscount = CalculateCappedDiscount(cicappedDiscountValues, cIBaseAndLoading);
                    decimal? CIBaselongTermDiscount = (cIBaseAndLoading - cIBasecappedDiscount) * tenureDiscount;

                    decimal? cIBaseBaseCoverPremium = cIBaseAndLoading - cIBasecappedDiscount - CIBaselongTermDiscount;

                    string cashBenefitOpt = "N";
                    decimal? hdcsi = 0;
                    if (siRiderTwoDataTable.Rows.Count >= 1)
                    {
                        foreach (DataRow itemRow in siRiderTwoDataTable.Rows)
                        {
                            cashBenefitOpt = "Y";
                            object hdcsiValueObject = itemRow["SIValue"];
                            hdcsi = Convert.ToDecimal(hdcsiValueObject);
                        }
                    }

                    var insuredRelationTAGValues = new List<string?>();
                    for (int i = 0; i < insuredRelations.Count; i++)
                    {
                        var insuredRelation = insuredRelations[i];
                        var insuredRelationTAG = relations
                            .Where(roww =>
                                roww.Value is Hashtable rateDetails &&
                                rateDetails["insured_relation"]?.ToString() == insuredRelation)
                            .Select(roww =>
                                roww.Value is Hashtable details && details["relation_tag"] != null
                                ? Convert.ToString(details["relation_tag"])
                                : (string?)null)
                            .FirstOrDefault();

                        insuredRelationTAGValues.Add(insuredRelationTAG);
                    }
                    // Perform the count and apply the logic
                    string aValue = ProcessCountForA(insuredRelationTAGValues);
                    string pValue = ProcessCountForP(insuredRelationTAGValues);
                    string cValue = ProcessCountForC(insuredRelationTAGValues);
                    string A = aValue;

                    var hdcAgeBand = hdcrates
                       .Where(roww =>
                           roww.Value is Hashtable rateDetails &&
                           rateDetails.ContainsKey("age") && rateDetails["age"] != null && (int)rateDetails["age"] == eldestMember &&
                           rateDetails.ContainsKey("plan_type") && rateDetails["plan_type"] != null && rateDetails["plan_type"].ToString() == A.ToString()
                       )
                       .Select(roww =>
                           roww.Value is Hashtable details &&
                           details.ContainsKey("age_band") && details["age_band"] != null
                           ? details["age_band"].ToString()
                           : null
                       )
                      .FirstOrDefault();

                    string? P = pValue;
                    string? C = cValue;
                    string? familyDefn = (A ?? string.Empty) + (P ?? string.Empty) + (C ?? string.Empty);
                    familyDefn = familyDefn.Trim();

                    // Construct the raw SQL query
                    var sqlperiod = $@"
 SELECT {columnName}
 FROM hdcrates
 WHERE si = @p0 AND age = @p1 AND age_band = @p2 AND plan_type=@p3";

                    // Execute the raw SQL query
                    var cashBenefitPremiumValue = hdcrates
                       .Where(roww =>
                           roww.Value is Hashtable rateDetails &&
                           rateDetails["si"] != null && (rateDetails["si"] as int? ?? 0) == hdcsi &&
                           rateDetails["age"] != null && (rateDetails["age"] as int? ?? 0) == eldestMember &&
                           rateDetails["age_band"] != null && rateDetails["age_band"].ToString() == hdcAgeBand &&
                           rateDetails["plan_type"] != null && rateDetails["plan_type"].ToString() == familyDefn
                       )
                       .Select(roww =>
                           roww.Value is Hashtable details && details[columnName] != null
                           ? Convert.ToDecimal(details[columnName])
                           : (decimal?)null
                       )
                       .FirstOrDefault();
                    decimal? cashBenefitSI = hdcsi;

                    string? ageBand = hdcAgeBand;
                    cashBenefitPremiumValue = cashBenefitPremiumValue ?? 0m;
                    decimal? cashBenefitPremium = cashBenefitPremiumValue;
                    // Construct the raw SQL query

                    var dictionary = new Dictionary<string, List<string>>
 {
     { "2a", GenerateColumnNames("2a") },
      { "2c", GenerateColumnNames("2c")},
       { "2a2p", Generate2A2PColumnNames("2a2p") },
       { "2a2c", Generate2A2PColumnNames("2a2c") }
 };

                    var key = familyDefn.ToLower();
                    var columnNamesString = "";
                    if (dictionary.TryGetValue(key, out var columnValues))
                    {
                        // Convert the list of column names to a single string
                        columnNamesString = string.Join(", ", columnValues);
                        Console.WriteLine($"Key: {key}");
                        Console.WriteLine($"Column Values: {columnNamesString}");
                    }
                    ////{columnNamesString},p1, c1, c2, c3, p2,eldest_member_age_band, family_composition
                    var sqlhdcproportionsplit = $@"
 SELECT * 
 FROM hdcproportionsplit
 WHERE  eldest_member_age_band = @p0 AND family_composition = @p1";
                    //Execute the raw SQL query

                    var results = hdcproportionsplit
                             .Where(roww =>
                             {
                                 if (roww.Value is Hashtable rateDetails)
                                 {
                                     // Check that "eldest_member_age_band" exists and is not null
                                     var eldestMemberAgeBand = rateDetails["eldest_member_age_band"];
                                     var deductible = rateDetails["family_composition"];

                                     return eldestMemberAgeBand != null &&
                                            eldestMemberAgeBand.ToString() == hdcAgeBand && // Correct comparison for "eldest_member_age_band"
                                            deductible != null &&
                                            deductible.ToString() == familyDefn; // Correct comparison for "deductible"
                                 }
                                 return false;
                             })
                         .Select(roww =>
                         {
                             if (roww.Value is Hashtable details)
                             {
                                 // Only return a result if the necessary keys exist
                                 return new
                                 {
                                     eldest_member_age_band = details["eldest_member_age_band"]?.ToString(), // Use null-conditional operator
                                     family_composition = details["family_composition"]?.ToString(),
                                     a1 = details["a1"]?.ToString(),
                                     a2 = details["a2"]?.ToString(),
                                     p1 = details["p1"]?.ToString(),
                                     p2 = details["p2"]?.ToString(),
                                     c1 = details["c1"]?.ToString(),
                                     c2 = details["c2"]?.ToString()
                                 };
                             }
                             return null; // Return null if `details` is not a Hashtable
                         })
                         .Where(details => details != null) // Filter out null results
                         .ToList(); // Get the results as a list
                    var selectedValues = results.Select(r => new
                    {
                        a1 = Convert.ToDecimal(r.a1),
                        a2 = Convert.ToDecimal(r.a2),
                        p1 = Convert.ToDecimal(r.p1),
                        p2 = Convert.ToDecimal(r.p2),
                        c1 = Convert.ToDecimal(r.c1),
                        c2 = Convert.ToDecimal(r.c2)
                    }).FirstOrDefault();


                    decimal? Insured_1 = selectedValues?.a1 * cashBenefitPremiumValue;
                    decimal? Insured_2 = selectedValues?.a2 * cashBenefitPremiumValue;
                    decimal? Insured_3 = 0m;
                    decimal? Insured_4 = 0m;
                    if (familyDefn == "2A 2P")
                    {
                        Insured_3 = selectedValues?.p1 * cashBenefitPremiumValue;
                        Insured_4 = selectedValues?.p2 * cashBenefitPremiumValue;
                    }
                    if (familyDefn == "2A 2C")
                    {
                        Insured_3 = selectedValues?.c1 * cashBenefitPremiumValue;
                        Insured_4 = (selectedValues?.c2) * cashBenefitPremiumValue;
                    }

                    decimal? Insured_5 = 0;
                    decimal? Insured_6 = 0;
                    decimal? Insured_7 = 0;
                    decimal? Insured_8 = 0;
                    decimal? Insured_9 = 0;
                    decimal? Insured_10 = 0;
                    decimal? Insured_11 = 0;
                    decimal? Insured_12 = 0;


                    List<decimal?> premiumCheckInsuredValues = new List<decimal?>();
                    premiumCheckInsuredValues.Add(Insured_1);
                    premiumCheckInsuredValues.Add(Insured_2);
                    premiumCheckInsuredValues.Add(Insured_3);
                    premiumCheckInsuredValues.Add(Insured_4);
                    premiumCheckInsuredValues.Add(Insured_5);
                    premiumCheckInsuredValues.Add(Insured_6);
                    premiumCheckInsuredValues.Add(Insured_7);
                    premiumCheckInsuredValues.Add(Insured_8);
                    premiumCheckInsuredValues.Add(Insured_9);
                    premiumCheckInsuredValues.Add(Insured_10);
                    premiumCheckInsuredValues.Add(Insured_11);
                    premiumCheckInsuredValues.Add(Insured_12);

                    decimal? cashBenefitLoadingPremiumSum = premiumCheckInsuredValues.Sum();
                    decimal? premiumCheck = cashBenefitLoadingPremiumSum - cashBenefitPremiumValue;

                    // Calculate the result and handle errors              
                    List<decimal?> cashBenefitLoadingPremiumValues = new List<decimal?>();
                    for (int i = 0; i < noOfMembers; i++)
                    {
                        decimal? insuredValue = premiumCheckInsuredValues[i]; // Fetch the insured value for the current member
                        decimal? basicLoadingRate = basicLoadingRates[i]; // Fetch the loading rate for the current member

                        decimal? cashBenefit = CalculateLoadingPremium(insuredValue, basicLoadingRate / 100);
                        cashBenefitLoadingPremiumValues.Add(cashBenefit);
                    }

                    decimal? cashBenefitLoadingPremium = cashBenefitLoadingPremiumValues.Sum();

                    decimal? hDCBaseAndLoading = cashBenefitPremium + cashBenefitLoadingPremium;
                    decimal? hDCBaseAndLoadingLoyaltyDiscount = hDCBaseAndLoading * cIloyaltyDiscount;
                    decimal? hDCBaseAndLoadingEmployeeDiscount = hDCBaseAndLoading * GetEmployeeDiscount(employeeDiscountValue);
                    onlineDiscountValue = onlineDiscountValue / 100;
                    decimal? hDCOnlineDisocuntValue = hDCBaseAndLoading * (onlineDiscountValue);//GetOnlineDiscount(noOfMembers);
                    decimal? hDCBaseAndLoadingOnlineDiscount = hDCOnlineDisocuntValue;
                    decimal? hDCBaseAndLoadingFamilyDiscount = hDCBaseAndLoading * (familyDiscountValue);
                    decimal?[] hDCcappedDiscountValues = new decimal?[]
                     {
             hDCBaseAndLoadingLoyaltyDiscount, // Value from cell C158
             hDCBaseAndLoadingEmployeeDiscount, // Value from cell C159
             hDCBaseAndLoadingOnlineDiscount, // Value from cell C160
             hDCBaseAndLoadingFamilyDiscount  // Value from cell C161
                     };

                    decimal? hDCCappedDiscount = CalculateCappedDiscount(hDCcappedDiscountValues, hDCBaseAndLoading);

                    decimal? hDClongTermDiscount = (hDCBaseAndLoading - hDCCappedDiscount) * tenureDiscount;

                    decimal? hDCBaseCoverPremium = hDCBaseAndLoading - hDCCappedDiscount - hDClongTermDiscount;

                    decimal? netPremium = (finalBasePremium + cIBaseBaseCoverPremium + hDCBaseCoverPremium);

                    decimal? GST = netPremium * 0.18m;

                    decimal? finalPremium = netPremium + GST;

                    decimal? selectedUpsellPremium = row.upsellpremium1 ?? row.upsellpremium2 ?? row.upsellpremium3 ?? row.upsellpremium4 ?? row.upsellpremium5 ?? 0;

                    upsellCrosscheck = selectedUpsellPremium - finalPremium;

                    os = new OptimaSecureRNE
                    {
                        prod_code = row.prod_code,
                        prod_name = row.prod_name,
                        policy_number = row.policy_number,
                        batchid = row.batchid,
                        customer_id = row.customer_id,
                        customername = row.customername,
                        txt_salutation = row.txt_salutation,
                        location_code = row.location_code,
                        txt_apartment = row.txt_apartment,
                        txt_street = row.txt_street,
                        txt_areavillage = row.txt_areavillage,
                        txt_citydistrict = row.txt_citydistrict,
                        txt_state = row.txt_state,
                        state_code = row.state_code,
                        state_regis = row.state_regis,
                        txt_pincode = row.txt_pincode,
                        txt_nationality = row.txt_nationality,
                        txt_mobile = row.txt_mobile,
                        txt_telephone = row.txt_telephone,
                        txt_email = row.txt_email,
                        txt_dealer_cd = row.txt_dealer_cd,//intermediary_code in gc mapping
                        imdname = row.imdname,//intermediary_name in gc
                        verticalname = row.verticalname,//psm_name in gc
                                                        //ssm_name = row.ssm_name,
                        txt_family = row.txt_family,
                        isrnflag = row.isrnflag,//chk
                        reference_num = row.reference_num,//proposal no in gc
                        split_flag = row.split_flag,
                        isvipflag = row.isvipflag,//chk 
                        optima_secure_gst = /*row.GST,*/
                    row.optima_secure_gst.HasValue ? Math.Round(row.optima_secure_gst.Value, 2) : (decimal?)null,

                        txt_insuredname1 = row.txt_insuredname1,
                        txt_insuredname2 = row.txt_insuredname2,
                        txt_insuredname3 = row.txt_insuredname3,
                        txt_insuredname4 = row.txt_insuredname4,
                        txt_insuredname5 = row.txt_insuredname5,
                        txt_insuredname6 = row.txt_insuredname6,
                        txt_insuredname7 = row.txt_insuredname7,
                        txt_insuredname8 = row.txt_insuredname8,
                        txt_insuredname9 = row.txt_insuredname9,
                        txt_insuredname10 = row.txt_insuredname10,
                        txt_insuredname11 = row.txt_insuredname11,
                        txt_insuredname12 = row.txt_insuredname12,

                        txt_insured_entrydate1 = row.txt_insured_entrydate1,//chk inceptiondate in gc
                        txt_insured_entrydate2 = row.txt_insured_entrydate2,
                        txt_insured_entrydate3 = row.txt_insured_entrydate3,
                        txt_insured_entrydate4 = row.txt_insured_entrydate4,
                        txt_insured_entrydate5 = row.txt_insured_entrydate5,
                        txt_insured_entrydate6 = row.txt_insured_entrydate6,
                        txt_insured_entrydate7 = row.txt_insured_entrydate7,
                        txt_insured_entrydate8 = row.txt_insured_entrydate8,
                        txt_insured_entrydate9 = row.txt_insured_entrydate9,
                        txt_insured_entrydate10 = row.txt_insured_entrydate10,
                        txt_insured_entrydate11 = row.txt_insured_entrydate11,
                        txt_insured_entrydate12 = row.txt_insured_entrydate12,

                        member_id1 = row.member_id1,
                        member_id2 = row.member_id2,
                        member_id3 = row.member_id3,
                        member_id4 = row.member_id4,
                        member_id5 = row.member_id5,
                        member_id6 = row.member_id6,
                        member_id7 = row.member_id7,
                        member_id8 = row.member_id8,
                        member_id9 = row.member_id9,
                        member_id10 = row.member_id10,
                        member_id11 = row.member_id11,
                        member_id12 = row.member_id12,

                        insured_loadingper1 = row.insured_loadingper1,
                        insured_loadingper2 = row.insured_loadingper2,
                        insured_loadingper3 = row.insured_loadingper3,
                        insured_loadingper4 = row.insured_loadingper4,
                        insured_loadingper5 = row.insured_loadingper5,
                        insured_loadingper6 = row.insured_loadingper6,
                        insured_loadingper7 = row.insured_loadingper7,
                        insured_loadingper8 = row.insured_loadingper8,
                        insured_loadingper9 = row.insured_loadingper9,
                        insured_loadingper10 = row.insured_loadingper10,
                        insured_loadingper11 = row.insured_loadingper11,
                        insured_loadingper12 = row.insured_loadingper12,

                        insured_loadingamt1 = row.insured_loadingamt1,
                        insured_loadingamt2 = row.insured_loadingamt2,
                        insured_loadingamt3 = row.insured_loadingamt3,
                        insured_loadingamt4 = row.insured_loadingamt4,
                        insured_loadingamt5 = row.insured_loadingamt5,
                        insured_loadingamt6 = row.insured_loadingamt6,
                        insured_loadingamt7 = row.insured_loadingamt7,
                        insured_loadingamt8 = row.insured_loadingamt8,
                        insured_loadingamt9 = row.insured_loadingamt9,
                        insured_loadingamt10 = row.insured_loadingamt10,
                        insured_loadingamt11 = row.insured_loadingamt11,
                        insured_loadingamt12 = row.insured_loadingamt12,

                        txt_insured_dob1 = row.txt_insured_dob1,
                        txt_insured_dob2 = row.txt_insured_dob2,
                        txt_insured_dob3 = row.txt_insured_dob3,
                        txt_insured_dob4 = row.txt_insured_dob4,
                        txt_insured_dob5 = row.txt_insured_dob5,
                        txt_insured_dob6 = row.txt_insured_dob6,
                        txt_insured_dob7 = row.txt_insured_dob7,
                        txt_insured_dob8 = row.txt_insured_dob8,
                        txt_insured_dob9 = row.txt_insured_dob9,
                        txt_insured_dob10 = row.txt_insured_dob10,
                        txt_insured_dob11 = row.txt_insured_dob11,
                        txt_insured_dob12 = row.txt_insured_dob12,


                        txt_insured_age1 = row.txt_insured_age1,
                        txt_insured_age2 = row.txt_insured_age2,
                        txt_insured_age3 = row.txt_insured_age3,
                        txt_insured_age4 = row.txt_insured_age4,
                        txt_insured_age5 = row.txt_insured_age5,
                        txt_insured_age6 = row.txt_insured_age6,
                        txt_insured_age7 = row.txt_insured_age7,
                        txt_insured_age8 = row.txt_insured_age8,
                        txt_insured_age9 = row.txt_insured_age9,
                        txt_insured_age10 = row.txt_insured_age10,
                        txt_insured_age11 = row.txt_insured_age11,
                        txt_insured_age12 = row.txt_insured_age12,

                        txt_insured_relation1 = row.txt_insured_relation1,//coming as "string"
                        txt_insured_relation2 = row.txt_insured_relation2,
                        txt_insured_relation3 = row.txt_insured_relation3,
                        txt_insured_relation4 = row.txt_insured_relation4,
                        txt_insured_relation5 = row.txt_insured_relation5,
                        txt_insured_relation6 = row.txt_insured_relation6,
                        txt_insured_relation7 = row.txt_insured_relation7,
                        txt_insured_relation8 = row.txt_insured_relation8,
                        txt_insured_relation9 = row.txt_insured_relation9,
                        txt_insured_relation10 = row.txt_insured_relation10,
                        txt_insured_relation11 = row.txt_insured_relation11,
                        txt_insured_relation12 = row.txt_insured_relation12,


                        insured_relation_tag_1 = row.insured_relation_tag_1,
                        insured_relation_tag_2 = row.insured_relation_tag_2,
                        insured_relation_tag_3 = row.insured_relation_tag_3,
                        insured_relation_tag_4 = row.insured_relation_tag_4,
                        insured_relation_tag_5 = row.insured_relation_tag_5,
                        insured_relation_tag_6 = row.insured_relation_tag_6,
                        insured_relation_tag_7 = row.insured_relation_tag_7,
                        insured_relation_tag_8 = row.insured_relation_tag_8,
                        insured_relation_tag_9 = row.insured_relation_tag_9,
                        insured_relation_tag_10 = row.insured_relation_tag_10,
                        insured_relation_tag_11 = row.insured_relation_tag_11,
                        insured_relation_tag_12 = row.insured_relation_tag_12,

                        pre_existing_disease1 = row.pre_existing_disease1,
                        pre_existing_disease2 = row.pre_existing_disease2,
                        pre_existing_disease3 = row.pre_existing_disease3,
                        pre_existing_disease4 = row.pre_existing_disease4,
                        pre_existing_disease5 = row.pre_existing_disease5,
                        pre_existing_disease6 = row.pre_existing_disease6,
                        pre_existing_disease7 = row.pre_existing_disease7,
                        pre_existing_disease8 = row.pre_existing_disease8,
                        pre_existing_disease9 = row.pre_existing_disease9,
                        pre_existing_disease10 = row.pre_existing_disease10,
                        pre_existing_disease11 = row.pre_existing_disease11,
                        pre_existing_disease12 = row.pre_existing_disease12,


                        insured_cb1 = row.insured_cb1,
                        insured_cb2 = row.insured_cb2,
                        insured_cb3 = row.insured_cb3,
                        insured_cb4 = row.insured_cb4,
                        insured_cb5 = row.insured_cb5,
                        insured_cb6 = row.insured_cb6,
                        insured_cb7 = row.insured_cb7,
                        insured_cb8 = row.insured_cb8,
                        insured_cb9 = row.insured_cb9,
                        insured_cb10 = row.insured_cb10,
                        insured_cb11 = row.insured_cb11,
                        insured_cb12 = row.insured_cb12,

                        basesumInsuredList = basesumInsuredList,
                        upsellsumInsuredList = upsellsumInsuredList,

                        insured_deductable1 = row.insured_deductable1,
                        insured_deductable2 = row.insured_deductable2,
                        insured_deductable3 = row.insured_deductable3,
                        insured_deductable4 = row.insured_deductable4,
                        insured_deductable5 = row.insured_deductable5,
                        insured_deductable6 = row.insured_deductable6,
                        insured_deductable7 = row.insured_deductable7,
                        insured_deductable8 = row.insured_deductable8,
                        insured_deductable9 = row.insured_deductable9,
                        insured_deductable10 = row.insured_deductable10,
                        insured_deductable11 = row.insured_deductable11,
                        insured_deductable12 = row.insured_deductable12,


                        wellness_discount1 = row.wellness_discount1,
                        wellness_discount2 = row.wellness_discount2,
                        wellness_discount3 = row.wellness_discount3,
                        wellness_discount4 = row.wellness_discount4,
                        wellness_discount5 = row.wellness_discount5,
                        wellness_discount6 = row.wellness_discount6,
                        wellness_discount7 = row.wellness_discount7,
                        wellness_discount8 = row.wellness_discount8,
                        wellness_discount9 = row.wellness_discount9,
                        wellness_discount10 = row.wellness_discount10,
                        wellness_discount11 = row.wellness_discount11,
                        wellness_discount12 = row.wellness_discount12,


                        stayactive1 = row.stayactive1,
                        stayactive2 = row.stayactive2,
                        stayactive3 = row.stayactive3,
                        stayactive4 = row.stayactive4,
                        stayactive5 = row.stayactive5,
                        stayactive6 = row.stayactive6,
                        stayactive7 = row.stayactive7,
                        stayactive8 = row.stayactive8,
                        stayactive9 = row.stayactive9,
                        stayactive10 = row.stayactive10,
                        stayactive11 = row.stayactive11,
                        stayactive12 = row.stayactive12,

                        coverbaseloadingrate1 = row.coverbaseloadingrate1,
                        coverbaseloadingrate2 = row.coverbaseloadingrate2,
                        coverbaseloadingrate3 = row.coverbaseloadingrate3,
                        coverbaseloadingrate4 = row.coverbaseloadingrate4,
                        coverbaseloadingrate5 = row.coverbaseloadingrate5,
                        coverbaseloadingrate6 = row.coverbaseloadingrate6,
                        coverbaseloadingrate7 = row.coverbaseloadingrate7,
                        coverbaseloadingrate8 = row.coverbaseloadingrate8,
                        coverbaseloadingrate9 = row.coverbaseloadingrate9,
                        coverbaseloadingrate10 = row.coverbaseloadingrate10,
                        coverbaseloadingrate11 = row.coverbaseloadingrate11,
                        coverbaseloadingrate12 = row.coverbaseloadingrate12,

                        health_incentive1 = row.health_incentive1,
                        health_incentive2 = row.health_incentive2,
                        health_incentive3 = row.health_incentive3,
                        health_incentive4 = row.health_incentive4,
                        health_incentive5 = row.health_incentive5,
                        health_incentive6 = row.health_incentive6,
                        health_incentive7 = row.health_incentive7,
                        health_incentive8 = row.health_incentive8,
                        health_incentive9 = row.health_incentive9,
                        health_incentive10 = row.health_incentive10,
                        health_incentive11 = row.health_incentive11,
                        health_incentive12 = row.health_incentive12,

                        fitness_discount1 = row.fitness_discount1,
                        fitness_discount2 = row.fitness_discount2,
                        fitness_discount3 = row.fitness_discount3,
                        fitness_discount4 = row.fitness_discount4,
                        fitness_discount5 = row.fitness_discount5,
                        fitness_discount6 = row.fitness_discount6,
                        fitness_discount7 = row.fitness_discount7,
                        fitness_discount8 = row.fitness_discount8,
                        fitness_discount9 = row.fitness_discount9,
                        fitness_discount10 = row.fitness_discount10,
                        fitness_discount11 = row.fitness_discount11,
                        fitness_discount12 = row.fitness_discount12,

                        reservbenefis1 = row.reservbenefis1,
                        reservbenefis2 = row.reservbenefis2,
                        reservbenefis3 = row.reservbenefis3,
                        reservbenefis4 = row.reservbenefis4,
                        reservbenefis5 = row.reservbenefis5,
                        reservbenefis6 = row.reservbenefis6,
                        reservbenefis7 = row.reservbenefis7,
                        reservbenefis8 = row.reservbenefis8,
                        reservbenefis9 = row.reservbenefis9,
                        reservbenefis10 = row.reservbenefis10,
                        reservbenefis11 = row.reservbenefis11,
                        reservbenefis12 = row.reservbenefis12,

                        insured_rb_claimamt1 = row.insured_rb_claimamt1,
                        insured_rb_claimamt2 = row.insured_rb_claimamt2,
                        insured_rb_claimamt3 = row.insured_rb_claimamt3,
                        insured_rb_claimamt4 = row.insured_rb_claimamt4,
                        insured_rb_claimamt5 = row.insured_rb_claimamt5,
                        insured_rb_claimamt6 = row.insured_rb_claimamt6,
                        insured_rb_claimamt7 = row.insured_rb_claimamt7,
                        insured_rb_claimamt8 = row.insured_rb_claimamt8,
                        insured_rb_claimamt9 = row.insured_rb_claimamt9,
                        insured_rb_claimamt10 = row.insured_rb_claimamt10,
                        insured_rb_claimamt11 = row.insured_rb_claimamt11,
                        insured_rb_claimamt12 = row.insured_rb_claimamt12,


                        preventive_hc = row.preventive_hc,
                        policy_start_date = row.policy_start_date,
                        policy_expiry_date = row.policy_expiry_date,
                        policy_type = row.policy_type,
                        policy_period = row.policy_period,
                        policyplan = row.policyplan,
                        claimcount = row.claimcount,
                        num_tot_premium = row.num_tot_premium.HasValue ? Math.Round(row.num_tot_premium.Value, 2) : (decimal?)null,

                        no_of_members = noOfMembers,
                        eldest_member = eldestMember,

                        tier_type = row.tier_type,

                        combi_discount = (combiDiscountValue * 100),
                        employee_discount = employeeDiscountValue,
                        online_discount = (onlineDiscountValue * 100),//
                        loyalty_discount = loyaltyDiscountValue,
                        tenure_discount = (tenureDiscount * 100),
                        loading_premium = loadingPrem,
                        family_discount = (familyDiscountValue * 100),
                        dedcutable_discount = deductibleDiscountVal,

                        //base_premium_1 = basePremium1,//chk
                        //base_premium_2 = basePremium2,
                        //base_premium_3 = basePremium3,
                        //base_premium_4 = basePremium4,
                        //base_premium_5 = basePremium5,
                        //base_premium_6 = basePremium6,
                        //base_premium_7 = basePremium7,
                        //base_premium_8 = basePremium8,
                        //base_premium_9 = basePremium9,
                        //base_premium_10 = basePremium10,
                        //base_premium_11 = basePremium11,
                        //base_premium_12 = basePremium12,
                        base_premium = basePremium,
                        base_premium_after_deductible = basePremiumAfterDeductible,

                        //first base loading
                        loading_premiums = loadingPremiumvalues,
                        //loading_prem2 = loadingPrem2,
                        //loading_prem3 = loadingPrem3,
                        //loading_prem4 = loadingPrem4,
                        //loading_prem5 = loadingPrem5,
                        //loading_prem6 = loadingPrem6,
                        //loading_prem7 = loadingPrem7,
                        //loading_prem8 = loadingPrem8,
                        //loading_prem9 = loadingPrem9,
                        //loading_prem10 = loadingPrem10,
                        //loading_prem11 = loadingPrem11,
                        //loading_prem12 = loadingPrem12,
                        loading_prem_total = loadingPremium,

                        //cash_benefit_loading_prem_1 = cashBenefitInsured_1,
                        //cash_benefit_loading_prem_2 = cashBenefitInsured_2,
                        //cash_benefit_loading_prem_3 = cashBenefitInsured_3,
                        //cash_benefit_loading_prem_4 = cashBenefitInsured_4,
                        //cash_benefit_loading_prem_5 = cashBenefitInsured_5,
                        //cash_benefit_loading_prem_6 = cashBenefitInsured_6,
                        //cash_benefit_loading_prem_7 = cashBenefitInsured_7,
                        //cash_benefit_loading_prem_8 = cashBenefitInsured_8,
                        //cash_benefit_loading_prem_9 = cashBenefitInsured_9,
                        //cash_benefit_loading_prem_10 = cashBenefitInsured_10,
                        //cash_benefit_loading_prem_11 = cashBenefitInsured_11,
                        //cash_benefit_loading_prem_12 = loadingPrem12,
                        cash_benefit_loading_prem_total = cashBenefitLoadingPremium,

                        baseAndLoading = BaseAndLoading,//chk all values coming 0
                        baseAndLoading_LoyaltyDiscount = BaseAndLoadingLoyaltyDiscount,
                        baseAndLoading_EmployeeDiscount = BaseAndLoadingEmployeeDiscount,
                        baseAndLoading_OnlineDiscount = BaseAndLoadingOnlineDiscount,
                        baseAndLoading_FamilyDiscount = BaseAndLoadingFamilyDiscount,
                        baseAndLoading_CombiDiscount = combiDiscount,
                        baseAndLoading_CapppedDiscount = cappedDiscount,
                        baseAndLoading_LongTermDiscount = longTermDiscount,
                        //baseAndLoading_OS_Base_Premium = oSBasePremium,
                        baseAndLoading_OS_Base_Premium = oSBasePremium.HasValue ? Math.Round(oSBasePremium.Value, 2) : (decimal?)null,

                        baseAndLoading_Unlimited_Restore = unlimitedRestore,
                        baseAndLoading_Final_Base_Premium = finalBasePremium,

                        //cash benefit loading
                        //loading_prem_1 = loadingPrem_1,
                        //loading_prem_2 = loadingPrem_2,
                        //loading_prem_3 = loadingPrem_3,
                        //loading_prem_4 = loadingPrem_4,
                        //loading_prem_5 = loadingPrem_5,
                        //loading_prem_6 = loadingPrem_6,
                        //loading_prem_7 = loadingPrem_7,
                        //loading_prem_8 = loadingPrem_8,
                        //loading_prem_9 = loadingPrem_9,
                        //loading_prem_10 = loadingPrem_10,
                        //loading_prem_11 = loadingPrem_11,
                        //loading_prem_12 = loadingPrem_12,
                        loading_prem = loadingPrem,


                        hDCBaseAndLoading = hDCBaseAndLoading,
                        HDC_BaseCoverPremium = hDCBaseCoverPremium,
                        HDC_LoyaltyDiscount = hDCBaseAndLoadingLoyaltyDiscount,
                        HDC_EmployeeDiscount = hDCBaseAndLoadingEmployeeDiscount,
                        HDC_OnlineDiscount = hDCOnlineDisocuntValue,
                        HDC_FamilyDiscount = hDCBaseAndLoadingFamilyDiscount,
                        HDC_CapppedDiscount = hDCCappedDiscount,
                        HDC_LongTermDiscount = hDClongTermDiscount,


                        CI_BaseAndLoading = cIBaseAndLoading,
                        CI_BaseCoverPremium = cIBaseBaseCoverPremium,
                        CI_LoyaltyDiscount = cIBaseLoyaltyDisocunt,
                        CI_EmployeeDiscount = cIBaseEmployeeDisocunt,
                        CI_OnlineDiscount = cIBaseOnlineDisocunt,
                        CI_FamilyDiscount = cIBaseFamilyDisocunt,
                        CI_CapppedDiscount = cIBasecappedDiscount,
                        CI_LongTermDiscount = CIBaselongTermDiscount,

                        cash_Benefit_A = A,
                        cash_Benefit_C = C,
                        cash_Benefit_Age_Band = ageBand,
                        cash_Benefit_SI = cashBenefitSI,
                        cash_Benefit_Family_Defn = familyDefn,
                        Cash_Benefit_Premium = cashBenefitPremium,
                        cash_Benefit_insuredList = premiumCheckInsuredValues,
                        //cash_Benefit_insured_1 = Insured_1,
                        //cash_Benefit_insured_2 = Insured_2,
                        //cash_Benefit_insured_3 = Insured_3,
                        //cash_Benefit_insured_4 = Insured_4,
                        //cash_Benefit_insured_5 = Insured_5,
                        //cash_Benefit_insured_6 = Insured_6,
                        //cash_Benefit_insured_7 = Insured_7,
                        //cash_Benefit_insured_8 = Insured_8,
                        //cash_Benefit_insured_9 = Insured_9,
                        //cash_Benefit_insured_10 = Insured_10,
                        //cash_Benefit_insured_11 = Insured_11,
                        //cash_Benefit_insured_12 = Insured_12,
                        cash_Benefit_Premium_Check = premiumCheck,

                        //loading_insured_1 = cashBenefitInsured_1,
                        //loading_insured_2 = cashBenefitInsured_2,
                        //loading_insured_3 = cashBenefitInsured_3,
                        //loading_insured_4 = cashBenefitInsured_4,
                        //loading_insured_5 = cashBenefitInsured_5,
                        //loading_insured_6 = cashBenefitInsured_6,
                        //loading_insured_7 = cashBenefitInsured_7,
                        //loading_insured_8 = cashBenefitInsured_8,
                        //loading_insured_9 = cashBenefitInsured_9,
                        //loading_insured_10 = cashBenefitInsured_10,
                        //loading_insured_11 = cashBenefitInsured_11,
                        //loading_insured_12 = cashBenefitInsured_12,

                        sum_insured1 = row.sum_insured1,
                        sum_insured2 = row.sum_insured2,
                        sum_insured3 = row.sum_insured3,
                        sum_insured4 = row.sum_insured4,
                        sum_insured5 = row.sum_insured5,
                        sum_insured6 = row.sum_insured6,
                        sum_insured7 = row.sum_insured7,
                        sum_insured8 = row.sum_insured8,
                        sum_insured9 = row.sum_insured9,
                        sum_insured10 = row.sum_insured10,
                        sum_insured11 = row.sum_insured11,
                        sum_insured12 = row.sum_insured12,

                        critical_Illness_AddOn_Premium = premium,//chk
                        critical_Illness_Add_On_Opt = Opt,
                        critical_Illness_Add_On_SI = SI,
                        //critical_Illness_Add_On_Premium1 = premium1,
                        //critical_Illness_Add_On_Premium2 = premium2,
                        //critical_Illness_Add_On_Premium3 = premium3,
                        //critical_Illness_Add_On_Premium4 = premium4,
                        //critical_Illness_Add_On_Premium5 = premium5,
                        //critical_Illness_Add_On_Premium6 = premium6,
                        //critical_Illness_Add_On_Premium7 = premium7,
                        //critical_Illness_Add_On_Premium8 = premium8,
                        //critical_Illness_Add_On_Premium9 = premium9,
                        //critical_Illness_Add_On_Premium10 = premium10,
                        //critical_Illness_Add_On_Premium11 = premium11,
                        //critical_Illness_Add_On_Premium12 = premium12,
                        critical_Illness_Add_On_PremiumList = premiums,
                        // ci_Variant = CIVariant,

                        cash_Benefit_Opt = Opt,


                        base_Loading_And_Discount_Final_BasePremium = BaseAndLoading,
                        base_Loading_And_Discount_Premium = finalBasePremium,
                        net_premium = row.num_net_premium,
                        final_Premium_upsell = finalPremium.HasValue ? Math.Round(finalPremium.Value, 2) : (decimal?)0,

                        // netPremium = netPremium,
                        netPremium = netPremium.HasValue ? Math.Round(netPremium.Value, 2) : (decimal?)0,
                        GST = GST.HasValue ? Math.Round(GST.Value, 2) : (decimal?)0,
                        baseprem_cross_Check = baseCrosscheck,
                        upsellbaseprem_cross_Check = upsellCrosscheck

                       };
                    }
                }
            return new List<OptimaSecureRNE> { os };
        }
        private async Task<decimal?> GetCIRate(int? insuredAge, decimal? CIVariant , string policyPeriod, string sqlpolicyPeriod,Dictionary<string, Hashtable> cirates)
        {
            var ciRate = cirates
                .Where(roww =>
                {
                    if (roww.Value is Hashtable rateDetails)
                    {
                        return rateDetails.ContainsKey("si") && rateDetails["si"] != null &&
                               rateDetails.ContainsKey("ci_variant") && rateDetails["ci_variant"] != null &&
                               (int)rateDetails["si"] == insuredAge &&
                               (int)rateDetails["ci_variant"] == CIVariant;
                    }
                    return false;
                })
                .Select(roww =>
                {
                    if (roww.Value is Hashtable details && details.ContainsKey(policyPeriod) && details[policyPeriod] != null)
                    {
                        return Convert.ToDecimal(details[policyPeriod]);
                    }
                    return 0m; // Default to 0 if the value is not found
                })
                .FirstOrDefault();  // This will return the first matching rate or 0 if not found

            return ciRate;
        }

        private static decimal? GetCiRatesValues(decimal? SI, decimal? ciRate)
        {
            var cirateval = (ciRate * SI) / 1000;
            if (cirateval <= 0)
            {
                cirateval = 0.00m;// 10% discount
            }
            return cirateval;
        }

        static void AddCurrentRow(DataTable table, OptimaSecureUpsellPremiumValidation.Models.Domain.OptimaSecureGC currentRowData)
        {
            DataRow newRow = table.NewRow();

            //foreach (var entry in currentRowData)
            //{
            //    // Check if the column exists
            //    if (table.Columns.Contains(entry.Key))
            //    {
            //        newRow[entry.Key] = entry.Value;
            //    }
            //}

            table.Rows.Add(newRow);
        }
        private static decimal? GetEmployeeDiscount(decimal? noOfMembers)
        {
            decimal? cIFamilyDiscountValue = 0m;
            if (noOfMembers > 1)
            {
                cIFamilyDiscountValue = 0.1m; // 2.5%
            }

            return cIFamilyDiscountValue;
        }
        private static decimal? GetFamilyDiscount(int? noOfMembers)
        {
            decimal? cIFamilyDiscountValue = 0m;
            if (noOfMembers > 1)
            {
                cIFamilyDiscountValue = 0.1m; // 2.5%
            }

            return cIFamilyDiscountValue;
        }
        private static int GetOnlineDiscount(int? noOfMembers)
        {
            var cIFamilyDiscountValue = 0;
            if (noOfMembers > 0)
            {
                cIFamilyDiscountValue = 10; // 2.5%
            }
            return cIFamilyDiscountValue;
        }
        private int? TryParseAge(string ageStr)
        {
            return int.TryParse(ageStr, out int age) ? (int?)age : null;
        }
        private static void GetLoyaltyDiscount(decimal? loyaltyDiscount, out decimal? cIloyaltyDiscountValue, out decimal? cIloyaltyDiscount)
        {
            cIloyaltyDiscountValue = loyaltyDiscount;
            cIloyaltyDiscount = 0m;
            // Apply the discount logic
            if (cIloyaltyDiscountValue > 0)
            {
                cIloyaltyDiscount = 0.1m; // 10%
            }
        }
        static DataTable GetRiderSI(DataTable table, string riderName)
        { // Create a new DataTable to hold the results
            DataTable resultTable = new DataTable();
            resultTable.Columns.Add("RiderName", typeof(string));
            resultTable.Columns.Add("SIValue", typeof(object));

            foreach (DataColumn column in table.Columns)
            {
                if (column.ColumnName.StartsWith("covername"))
                {
                    // Construct the corresponding SI column name
                    string siColumnName = column.ColumnName.Replace("name", "si");

                    // Check for matching rider name
                    foreach (DataRow row in table.Rows)
                    {
                        if (row[column].ToString() == riderName)
                        {
                            //return row[siColumnName].ToString();
                            // Add the rider name and corresponding SI value to the result table
                            DataRow newRow = resultTable.NewRow();
                            newRow["RiderName"] = riderName;
                            newRow["SIValue"] = row[siColumnName];
                            resultTable.Rows.Add(newRow);
                        }
                    }
                }
            }
            return resultTable;
        }
        // Method to calculate the premium
        private decimal CalculateLoadingPremium(decimal? insured_1, decimal? basicLoadingRateOne)
        {
            try
            {
                // Check if either value is null
                if (!insured_1.HasValue || !basicLoadingRateOne.HasValue)
                {
                    return 0;
                }

                // Calculate the result
                decimal premium = insured_1.Value * basicLoadingRateOne.Value;

                // Return the calculated premium
                return premium;
            }
            catch (Exception)
            {
                // Return 0 in case of any error
                return 0;
            }
        }
        static decimal? CalculatelongTermDiscount(decimal? BaseAndLoading, decimal? cappedDiscount, decimal? combiDiscount, decimal? tenureDiscount)
        {
            // Perform the calculation (C157 - C162 - C163) * C129
            decimal? intermediate = BaseAndLoading - cappedDiscount - combiDiscount;
            decimal? result = intermediate * tenureDiscount;
            return result;
        }
        static decimal? CalculateCappedDiscount(decimal?[] cappedDiscountValues, decimal? BaseAndLoading)
        {
            // Calculate the sum of values in the range C158:C161
            decimal? sumOfDiscount = cappedDiscountValues.Sum();

            // Calculate 20% of the value in C157
            decimal? twentyPercentOfBaseAndLoading = BaseAndLoading * 0.20m;

            // Return the minimum of the two calculated values
            return MinDecimal(sumOfDiscount, twentyPercentOfBaseAndLoading);
        }
        //// Custom method to find the minimum of two decimal values
        static decimal? MinDecimal(decimal? a, decimal? b)
        {
            return (a < b) ? a : b;
        }
        private static List<string> GenerateColumnNames(string key)
        {
            if (string.IsNullOrEmpty(key) || key.Length < 2)
            {
                throw new ArgumentException("Invalid key format", nameof(key));
            }

            // Extract numeric and alphabetic parts
            string numberPart = key.Substring(0, 1); // First character
            string letterPart = key.Substring(1); // Remaining characters

            // Convert numberPart to integer
            if (!int.TryParse(numberPart, out int count))
            {
                throw new ArgumentException("Invalid numeric part in key", nameof(key));
            }

            // Generate column names based on the letterPart and count
            var columnNames = new List<string>();
            for (int i = 1; i <= count; i++)
            {
                columnNames.Add($"{letterPart}{i}");
            }

            return columnNames;
        }
        private static List<string> Generate2A2PColumnNames(string key)
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentException("Key cannot be null or empty", nameof(key));
            }

            var columnNames = new List<string>();

            // Use regex to match <number><letter> patterns
            var regex = new Regex(@"(\d+)([A-Za-z])");
            var matches = regex.Matches(key);

            foreach (Match match in matches)
            {
                if (match.Groups.Count == 3)
                {
                    string numberPart = match.Groups[1].Value; // Numeric part
                    string letterPart = match.Groups[2].Value; // Letter part

                    if (int.TryParse(numberPart, out int count))
                    {
                        for (int i = 1; i <= count; i++)
                        {
                            columnNames.Add($"{letterPart}{i}");
                        }
                    }
                }
            }

            return columnNames;
        }
        static decimal? CalculateLoadingPrem(decimal? basepremium1, decimal? base_loading_insured_1)
        {
            try
            {
                // Perform the calculation
                return basepremium1 * base_loading_insured_1;
            }
            catch (Exception)
            {
                // Handle any errors that occur during the calculation
                return 0;
            }
        }
        // Function to map policy_period to column name
        private static string GetColumnNameForPolicyPeriod(string policyPeriod)
        {
            return policyPeriod switch
            {
                "1 Year" => "one_year",
                "2 Years" => "two_years",
                "3 Years" => "three_years",
                _ => null,
            };
        }
        private decimal? CalculateResult(string condition, List<decimal?> values)
        {
            //if (values == null || !values.Any())
            //{
            //    throw new ArgumentException("Values list cannot be null or empty.");
            //}

            decimal? sum = values.Sum();
            decimal? max = values.Max();
            decimal? difference = sum - max;
            decimal? percentageAdjustment = difference * 0.45m;

            if (condition == "INDIVIDUAL")
            {
                return sum;
            }
            else
            {
                return max + percentageAdjustment;
            }
        }
        //private static IEnumerable<object> FindColumnValues<T>(IEnumerable<T> data, string columnName)
        //{
        //    // Get the type of the items in the collection
        //    Type type = typeof(T);

        //    // Get the property info for the specified column name
        //    PropertyInfo property = type.GetProperty(columnName, BindingFlags.Public | BindingFlags.Instance);

        //    // Check if the property exists
        //    if (property == null)
        //    {
        //        throw new ArgumentException($"Property '{columnName}' does not exist in type '{type.Name}'.");
        //    }

        //    // Extract values for the specified property
        //    return data.Select(item => property.GetValue(item));
        //}
        private string ProcessCountForA(List<string> cellValues)
        {
            if (cellValues == null)
            {
                return string.Empty;
            }
            // Count the number of entries starting with "A"
            int count = cellValues.Count(value => value != null && value.StartsWith("A", StringComparison.OrdinalIgnoreCase));

            // Return the formatted result based on the count
            return count > 0 ? $"{count}A" : string.Empty;
        }
        static string ProcessCountForP(List<string> cellValues)
        {
            if (cellValues == null)
            {
                return string.Empty;
            }
            // Count the number of entries starting with "P"
            int count = cellValues.Count(value => value != null && value.StartsWith("P", StringComparison.OrdinalIgnoreCase));

            // Return the formatted result based on the count
            return count > 0 ? $" {count}P" : string.Empty;
        }
        static string ProcessCountForC(List<string> cellValues)
        {
            if (cellValues == null)
            {
                return string.Empty;
            }
            // Count the number of entries starting with "P"
            int count = cellValues.Count(value => value != null && value.StartsWith("C", StringComparison.OrdinalIgnoreCase));

            // Return the formatted result based on the count
            return count > 0 ? $" {count}C" : string.Empty;
        }
        private decimal? CalculateResultMyBaseLoading(decimal? premium, decimal? basicLoadingRate)
        {
            try
            {
                //vlookupValue* c169 / 1000;
                // Convert percentage to decimal by dividing by 100
                decimal? result = premium * (basicLoadingRate / 100);

                return result;
            }
            catch (Exception)
            {
                // Return 0 if any error occurs
                return 0;
            }
        }
        private static decimal CalculateFamilyDiscount(string policyType, int numberOfMembers)
        {
            // Check the conditions and return the appropriate discount
            if (policyType == "INDIVIDUAL" && numberOfMembers > 1)
            {
                return 0.1m; // 10% discount
            }
            else
            {
                return 0.00m; // 0% discount
            }
        }
        private static decimal CalculateCombiDiscount(decimal? combiDiscount)
        {
            // Check the conditions and return the appropriate discount
            if (combiDiscount > 0)
            {
                return 0.055m; // 5.5% discount
            }
            else
            {
                return 0.00m; // 0% discount
            }
        }
        private static decimal GetPolicyPercentage(string policyPeriod)
        {
            // Apply the nested IF logic
            if (policyPeriod == "2 Years")
            {
                return 0.075m; // 7.5%
            }
            else if (policyPeriod == "3 Years")
            {
                return 0.10m; // 10%
            }
            else
            {
                return 0.00m; // 0%
            }
        }
        private static IEnumerable<object> GetValuesByColumnName(List<Dictionary<string, object>> data, string columnName)
        {
            // Return the values for the specified column name
            return data.Select(dict => dict.TryGetValue(columnName, out var value) ? value : null);
        }

        private async Task<List<string>> GetNonNullableRiderNameColumnsAsync()
        {
            // Define the query to get column names starting with 'rider_name'
            var columnQuery = "SELECT column_name AS ColumnName " +
                              "FROM information_schema.columns " +
                              "WHERE table_name = 'gc_hdfc' AND column_name LIKE 'covername%'  OR column_name LIKE 'coversi%'";

            var nonNullableColumns = new List<string>();
            var connectionString =
           System.Configuration.ConfigurationManager.
            ConnectionStrings["defaultConnection"].ToString();
            using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();

                // Get column names
                var columnNames = await connection.QueryAsync<ColumnNameDto>(columnQuery);

                // For each column, check if it has non-null values
                foreach (var column in columnNames)
                {
                    var checkQuery = $"SELECT COUNT(*) FROM gc_hdfc WHERE \"{column.ColumnName}\" IS NOT NULL";

                    var nonNullCount = await connection.ExecuteScalarAsync<int>(checkQuery);

                    if (nonNullCount > 0)
                    {
                        nonNullableColumns.Add(column.ColumnName);
                    }
                }
            }

            return nonNullableColumns;
        }
        static async Task<List<Dictionary<string, object>>> FetchDataAsync(string connectionString, List<string> columnNames)
        {
            var result = new List<Dictionary<string, object>>();

            using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();

                // Construct the SELECT query with dynamic column names
                var columns = string.Join(", ", columnNames);
                var selectQuery = $"SELECT {columns} FROM gc_hdfc";

                using (var command = new NpgsqlCommand(selectQuery, connection))
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var row = new Dictionary<string, object>();
                        foreach (var columnName in columnNames)
                        {
                            row[columnName] = reader[columnName];
                        }
                        result.Add(row);
                    }
                }
            }

            return result;
        }
        //public static List<string> SelectPolicies
        //   (string postgreTableName, List<string> newBatchIds,
        //    NpgsqlConnection postgresConnection, List<string> idPlaceholders)
        //{
        //    List<string> policies = new List<string>();
        //    idPlaceholders.Clear();
        //    foreach (var batchId in newBatchIds)
        //    {
        //        //var properties = (IDictionary<string, object>)item;

        //        //// Assuming 'BATCHID' is the key you want to extract
        //        //if (properties.TryGetValue("BATCHID", out var batchId))
        //        //{
        //        // Add the value to the idPlaceholders list (ensure it's a string)
        //        idPlaceholders.Add(batchId);
        //        //}
        //    }
        //    string status = "Reconciliation Successful";

        //    // Create the SQL query
        //    string query = $"SELECT policy_number  FROM {postgreTableName} WHERE rn_generation_status = {status}";

        //    // Fetch data from Oracle
        //    var oracleData = postgresConnection.Query<rne_requestedbatchpolicies>(query);
        //    if (oracleData.Any())
        //    {
        //        //using (transaction = postgresConnection.BeginTransaction())
        //        //{
        //        try
        //        {                    
        //            foreach (var row in oracleData)
        //            {
        //                // Dynamically create the insert command
        //                var properties = typeof(rne_requestedbatchpolicies).GetProperties();
        //                var columns = string.Join(", ", properties.Select(p => p.Name));
        //                var parameters = string.Join(", ", properties.Select(p => "@" + p.Name));

        //                //var insertQuery = $"INSERT INTO ins.{postgreTableName}({columns}) VALUES ({parameters});";
        //                //postgresConnection.Execute(insertQuery, row, transaction);
        //                policies.Add(row.policynumber);
        //            }                   
        //            Console.WriteLine("Data transferred successfully to rne_requestedbatchpolicies in postgre!");
        //            return policies;
        //        }
        //        catch (Exception ex)
        //        {
        //            // Roll back the transaction on error
        //            //transaction.Rollback();
        //            Console.WriteLine(ex.ToString());
        //            return null;
        //        }
        //        //}
        //    }
        //    //if (oracleData == null || !oracleData.Any())
        //    //{
        //    //    Console.WriteLine("No data returned from the rne_requestedbatchpoliciesquery.");
        //    //}

        //    //// Print the query for debugging purposes
        //    //Console.WriteLine(query);
        //    return policies;
        //}
        public List<List<string>> FetchNewBatchIds(NpgsqlConnection postgresConnection)
        {
            string? status = ConfigurationManager.AppSettings["Status"];
            var sqlSource = $"SELECT distinct ir.certificate_no, ir.product_code FROM ins.idst_renewal_data_rgs ir " +
                $"INNER JOIN ins.rne_healthtab ht" +
                $" ON ir.certificate_no = ht.policy_number " +
                $"LEFT JOIN ins.premium_validation pt3 ON ir.certificate_no = pt3.certificate_no " +
                $"WHERE ir.rn_generation_status = @Status AND ht.prod_code = 2856 " +
                $" AND pt3.rn_generation_status IS NULL " +
                $"AND (upselltype1='SI_UPSELL' OR upselltype2='SI_UPSELL' OR upselltype3 ='SI_UPSELL' " +
                $"OR upselltype4='SI_UPSELL' OR upselltype5='SI_UPSELL' OR upselltype1='UPSELLBASESI_1'" +
                $"OR upselltype2 ='UPSELLBASESI_1' OR upselltype3 = 'UPSELLBASESI_1' OR upselltype4 = 'UPSELLBASESI_1' OR upselltype5 = 'UPSELLBASESI_1') ";
            var sourceResults = postgresConnection.Query(sqlSource, new { Status = status });
            var sourceBatchIds = new List<List<string>>();
            foreach (var result in sourceResults)
            {
                var batchInfo = new List<string> { result.certificate_no, result.product_code.ToString() };
                sourceBatchIds.Add(batchInfo);
            }
            return sourceBatchIds;
        }

        public List<string> FetchNewBatchIdfrompostgre(string oracleschemaName, OracleConnection oracleConnection, NpgsqlConnection postgresConnection)
        {
            Console.WriteLine("FetchNewBatchIdfrompostgre method started");
            string status = ConfigurationManager.AppSettings["Status"];
            // Fetch source batch IDs
            var sourceBatchIds = new HashSet<string>();
            var sqlSource = $"SELECT DISTINCT batchid FROM ins.tablea";
            var sourceResults = postgresConnection.Query<string>(sqlSource);
            foreach (string batchId in sourceResults)
            {
                sourceBatchIds.Add(batchId);
            }

            // Fetch target batch IDs
            var targetBatchIds = new HashSet<string>();
            var sqlTarget = "SELECT DISTINCT batchid FROM ins.tableb"; // Adjust for your PostgreSQL schema if needed
            var targetResults = postgresConnection.Query<string>(sqlTarget);
            foreach (string batchId in targetResults)
            {
                targetBatchIds.Add(batchId);
            }

            // Calculate new batch IDs
            sourceBatchIds.ExceptWith(targetBatchIds);
            Console.WriteLine("FetchNewBatchIdfrompostgre method completed");
            return new List<string>(sourceBatchIds);

        }
        public void SelectAndinsertRequestedBatch(string oracleTableName, string postgreTableName, string oracleschemaName, List<string> newBatchIds, OracleConnection oracleConnection, NpgsqlConnection postgresConnection, NpgsqlTransaction transaction, List<string> idPlaceholders)
        {
            Console.WriteLine("SelectAndinsertRequestedBatch method started");
            string status = ConfigurationManager.AppSettings["Status"];
            idPlaceholders.Clear();
            foreach (var batchId in newBatchIds)
            {

                //var properties = (IDictionary<string, object>)item;

                //// Assuming 'BATCHID' is the key you want to extract
                //if (properties.TryGetValue("BATCHID", out var batchId))
                //{
                // Add the value to the idPlaceholders list (ensure it's a string)
                idPlaceholders.Add(batchId);
                //}
            }

            // Create the SQL query
            string query = $"SELECT * FROM ins.tablea WHERE BATCHID IN ({string.Join(",", idPlaceholders.Select(id => $"'{id}'"))})";

            // Fetch data from Oracle
            var oracleData = postgresConnection.Query<tablea>(query);
            if (oracleData.Any())
            {
                //using (transaction = postgresConnection.BeginTransaction())
                //{
                try
                {
                    foreach (var row in oracleData)
                    {
                        // Dynamically create the insert command
                        var properties = typeof(tablea).GetProperties();
                        var columns = string.Join(", ", properties.Select(p => p.Name));
                        var parameters = string.Join(", ", properties.Select(p => "@" + p.Name));

                        var insertQuery = $"INSERT INTO ins.tableb({columns}) VALUES ({parameters});";
                        postgresConnection.Execute(insertQuery, row, transaction);
                        ////Commit the transaction

                    }
                    //transaction.Commit();
                    Console.WriteLine("Data transferred successfully to tablea in postgre!");
                }

                catch (Exception ex)
                { // Roll back the transaction on error
                  //transaction.Rollback();
                    Console.WriteLine(ex.ToString());
                }
                // }
            }

            if (oracleData == null || !oracleData.Any())
            {
                Console.WriteLine("No data returned from the rne_requestedbatchpoliciesquery.");
            }
            // Print the query for debugging purposes
            Console.WriteLine(query);
            Console.WriteLine("SelectAndinsertRequestedBatch method completed");
        }
        public async Task<Dictionary<string, Hashtable>> GetRatesAsync(HDFCDbContext dbContext)
        {
            // Use a Dictionary to store rates with compositeKey as the key
            var ratesTable = new Dictionary<string, Hashtable>();


            // Fetch rates from the database
            var rates = await dbContext.baserate.ToListAsync().ConfigureAwait(false);

            // Loop through each rate and add to the dictionary
            foreach (var rate in rates)
            {
                // Create a composite key (e.g., "si-age")
                var compositeKey = $"{rate.si}-{rate.age}-{rate.tier}-{rate.product}";

                // Create rate details
                var rateDetails = new Hashtable
                {
                    { "si", rate.si },
                    { "age", rate.age },
                    { "tier", rate.tier },
                    { "product", rate.product },
                    { "one_year", rate.one_year },
                    { "two_years", rate.two_years },
                    { "three_years", rate.three_years }
                };

                // Add the rate details to the dictionary using compositeKey as the key
                ratesTable[compositeKey] = rateDetails;
            }

            // Return the dictionary containing rate details
            return ratesTable;
        }
        public async Task<Dictionary<string, Hashtable>> GetRelationTagsAsync(HDFCDbContext dbContext)
        {
            var relationTagsTable = new Dictionary<string, Hashtable>();

            var relations = await dbContext.relations.ToListAsync().ConfigureAwait(false);
            foreach (var relation in relations)
            {
                var compositeKey = $"{relation.insured_relation}-{relation.relation_tag}";// In this case, just using insured_relation as key

                var relationDetails = new Hashtable
                {
                    { "insured_relation", relation.insured_relation },
                    { "relation_tag", relation.relation_tag }
        };

                relationTagsTable[compositeKey] = relationDetails;
            }
            return relationTagsTable;
        }
        public async Task<Dictionary<string, Hashtable>> GetCIRatesTagsAsync(HDFCDbContext dbContext)
        {
            var ciratesTable = new Dictionary<string, Hashtable>();
            var cirates = await dbContext.cirates.ToListAsync().ConfigureAwait(false);

            foreach (var cirate in cirates)
            {
                var compositeKey = $"{cirate.age}-{cirate.ci_variant}";// In this case, just using insured_relation as key
                var cirateDetails = new Hashtable
                {
                    { "age", cirate.age },
                    { "ci_variant", cirate.ci_variant },
                    { "one_year", cirate.one_year },
                    { "two_years", cirate.two_years },
                    { "three_years", cirate.three_years }
                };
                ciratesTable[compositeKey] = cirateDetails;
            }
            return ciratesTable;
        }
        public async Task<Dictionary<string, Hashtable>> GetCARatesTagsAsync(HDFCDbContext dbContext)
        {
            var caratesTable = new Dictionary<string, Hashtable>();
            var carates = await dbContext.carates.ToListAsync().ConfigureAwait(false);

            foreach (var carate in carates)
            {
                var compositeKey = $"{carate.age}-{carate.age_band}-{carate.si}";// In this case, just using insured_relation as key
                var carateDetails = new Hashtable
                {
                    { "si", carate.si },
                    { "age", carate.age },
                    { "one_year", carate.one_year },
                    { "two_years", carate.two_years },
                    { "three_years", carate.three_years }
                };
                caratesTable[compositeKey] = carateDetails;
            }
            return caratesTable;
        }
        public async Task<Dictionary<string, Hashtable>> GetHDCRatesTagsAsync(HDFCDbContext dbContext)
        {
            var hdcratesTable = new Dictionary<string, Hashtable>();
            var hdcrates = await dbContext.hdcrates.ToListAsync().ConfigureAwait(false);
            foreach (var hdcrate in hdcrates)
            {
                var compositeKey = $"{hdcrate.age}-{hdcrate.age_band}-{hdcrate.si}-{hdcrate.plan_type}";// In this case, just using insured_relation as key
                var hdcratesDetails = new Hashtable
                {
                    { "si", hdcrate.si },
                    { "age", hdcrate.age },
                    { "age_band" ,hdcrate.age_band},
                    { "plan_type", hdcrate.plan_type },
                    { "one_year", hdcrate.one_year },
                    { "two_years", hdcrate.two_years },
                    { "three_years", hdcrate.three_years }
                };
                hdcratesTable[compositeKey] = hdcratesDetails;
            }
            return hdcratesTable;
        }
        public async Task<Dictionary<string, Hashtable>> GetHDCProportionSplitTagsAsync(HDFCDbContext dbContext)
        {
            var hdcproportionsplitTable = new Dictionary<string, Hashtable>();
            var hdcproportionsplit = await dbContext.hdcproportionsplit.ToListAsync().ConfigureAwait(false);

            foreach (var hdcrate in hdcproportionsplit)
            {
                var compositeKey = $"{hdcrate.eldest_member_age_band}-{hdcrate.family_composition}-{hdcrate.a1}-{hdcrate.a2}-{hdcrate.p1}-{hdcrate.c1}-{hdcrate.c2}-{hdcrate.c3}-{hdcrate.p2}";// In this case, just using insured_relation as key
                var hdcratesDetails = new Hashtable
                {
                    { "eldest_member_age_band", hdcrate.eldest_member_age_band },
                    { "family_composition", hdcrate.family_composition },
                    { "a1", hdcrate.a1 },
                    { "a2", hdcrate.a2 },
                    { "p1", hdcrate.p1 },
                    { "c1", hdcrate.c1 },
                     { "c2", hdcrate.c1 },
                      { "c3", hdcrate.c1 },
                       { "p2", hdcrate.c1 }
                };
                hdcproportionsplitTable[compositeKey] = hdcratesDetails;
            }
            return hdcproportionsplitTable;
        }
        public async Task<Dictionary<string, Hashtable>> GetDedutableDiscountAsync(HDFCDbContext dbContext)
        {
            var deductiblediscountTable = new Dictionary<string, Hashtable>();
            var deductiblediscount = await dbContext.deductiblediscount.ToListAsync().ConfigureAwait(false);

            foreach (var relation in deductiblediscount)
            {
                var compositeKey = $"{relation.si}-{relation.deductible}-{relation.discount}";// In this case, just using insured_relation as key
                var relationDetails = new Hashtable
                {
                    { "si", relation.si },
                    { "deductible", relation.deductible },
                     { "discount", relation.discount }
                };
                deductiblediscountTable[compositeKey] = relationDetails;
            }
            return deductiblediscountTable;
        }

    }
    public class verifiedpremiumvalues
    {
        public decimal? verified_gst { get; set; }
        public decimal? verified_total_premium { get; set; }
        public decimal? verified_net_premium { get; set; }
        public decimal? crosscheck { get; set; }
    }
    public class IdstData
    {
        public string certificate_no { get; set; }
        public decimal? loading_per_insured1 { get; set; }
        public decimal? loading_per_insured2 { get; set; }
        public decimal? loading_per_insured3 { get; set; }
        public decimal? loading_per_insured4 { get; set; }
        public decimal? loading_per_insured5 { get; set; }

        public decimal? loading_per_insured6 { get; set; }
        public decimal? loading_per_insured7 { get; set; }
        public decimal? loading_per_insured8 { get; set; }
        public decimal? loading_per_insured9 { get; set; }

        public decimal? loading_per_insured10 { get; set; }
        public decimal? loading_per_insured11 { get; set; }
        public decimal? loading_per_insured12 { get; set; }


        // Other properties...
    }
}
