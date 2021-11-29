using MicroRabbit.Domain.Core.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroRabbit.Banking.Domain.Commands
{
    public abstract class TransferCommand:Command
    {
        public int From { get; set; }
        public int To { get; set; }
        public decimal Amount { get; set; }
    }
}
