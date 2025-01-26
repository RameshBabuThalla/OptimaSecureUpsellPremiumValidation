﻿using System.ComponentModel.DataAnnotations;

namespace OptimaSecureUpsellPremiumValidation.Models.Domain
{
    public class Vehicle_Code
    {
        [Key]
        public string vehicle_category { get; set; }
        public int vehicle_code { get; set; }
    }
}
