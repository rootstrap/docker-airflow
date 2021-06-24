#create-sbn.tf
 
resource "aws_subnet" "subnet" {
  vpc_id                  = aws_vpc.vpc.id
  cidr_block              = var.sbn_cidr_block
  map_public_ip_on_launch = var.sbn_public_ip
 
  tags = {
    "Owner" = var.owner
    "Name"  = "${var.owner}-subnet"
  }
}
