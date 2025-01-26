using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace OptimaSecureUpsellPremiumValidation
{
    internal class EmailService
    {
        public static void SendEmail()
        {
            
            try
            {
                // Create a new MailMessage object
                var mailMessage = new MailMessage();
                string from = ConfigurationManager.AppSettings["FromEmail"];
                // Set up the sender email address
                mailMessage.From = new MailAddress(from);  // Sender email

                // Email addresses separated by comma
                string recipients = ConfigurationManager.AppSettings["ToEmail"];

                // Split the comma-separated string and add each recipient
                foreach (var email in recipients.Split(','))
                {
                    mailMessage.To.Add(email.Trim());
                }

                // Set up the subject and body of the email
                mailMessage.Subject = ConfigurationManager.AppSettings["EmailSubject"];
                mailMessage.Body = ConfigurationManager.AppSettings["EmailBody"];

                // Set up the SMTP client to send the email
                var smtpClient = new SmtpClient(ConfigurationManager.AppSettings["EmailServer"])
                {
                    Port = 587,

                    Credentials = new NetworkCredential(ConfigurationManager.AppSettings["FromEmailhdfc"], ConfigurationManager.AppSettings["Password"]), // Your Outlook credentials
                    EnableSsl = true // SSL should be enabled
                };

                // Send the email
                smtpClient.Send(mailMessage);

                Console.WriteLine("Email sent successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred: {ex.Message}");
            }
        }
    }
    

    }

