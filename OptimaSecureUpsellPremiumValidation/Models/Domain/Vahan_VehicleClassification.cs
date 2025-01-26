using System.ComponentModel.DataAnnotations;

namespace OptimaSecureUpsellPremiumValidation.Models.Domain
{
    public class Vahan_VehicleClassification
    {
        [Key]
        public int id { get; set; }
        public string? vahan_vehicle_class { get; set; }
        public string? vahan_category { get; set; }
        public string? hdfcergo_vehicle_class { get; set; }
        public int? vehicle_code { get; set; }
    }
}
