using System.ComponentModel.DataAnnotations;

namespace OptimaSecureUpsellPremiumValidation.Models.Domain
{
    public class workflow_status_master
    {
        [Key]
        public int workflow_statusid
        { get; set; }
        public string rn_generation_status_name
        { get; set; }
    }
}
