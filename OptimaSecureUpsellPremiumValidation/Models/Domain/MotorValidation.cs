﻿using System.ComponentModel.DataAnnotations;

namespace OptimaSecureUpsellPremiumValidation.Models.Domain
{
    public class MotorValidation
    { 
        public string? final_remark { get; set; }
        public string? dispatch_status { get; set; }
        public string? error_description { get; set; }
    }
}
