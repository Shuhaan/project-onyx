# ---------------
# Lambda IAM Role
# ---------------

# Define
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Create
resource "aws_iam_role" "extract_lambda_role" {
  name_prefix        = "role-${var.extract_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

resource "aws_iam_role" "load_lambda_role" {
  name_prefix        = "role-${var.load_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# Create
resource "aws_iam_role" "transform_lambda_role" {
  name_prefix        = "role-${var.transform_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}


# ------------------------------------
# Extract Lambda IAM Policy for S3 Write
# ------------------------------------

# Define
data "aws_iam_policy_document" "s3_extract_data_policy_doc" {
  statement {
    actions = ["s3:PutObject", "s3:ListBucket", "s3:GetObject"]
    resources = [
      "${aws_s3_bucket.ingested_data_bucket.arn}",
      "${aws_s3_bucket.ingested_data_bucket.arn}/*"
    ]
    effect = "Allow"
  }
}

data "aws_iam_policy_document" "s3_load_data_policy_doc" {
  statement {
    actions = ["s3:PutObject", "s3:ListBucket", "s3:GetObject"]
    resources = [
      "${aws_s3_bucket.ingested_data_bucket.arn}",
      "${aws_s3_bucket.ingested_data_bucket.arn}/*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "s3_extract_write_policy" {
  name_prefix = "s3-policy-${var.extract_lambda}-read-and-write"
  policy      = data.aws_iam_policy_document.s3_extract_data_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_extract_write_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.s3_extract_write_policy.arn
}

resource "aws_iam_policy" "s3_load_write_policy" {
  name_prefix = "s3-policy-${var.load_lambda}-read-and-write"
  policy      = data.aws_iam_policy_document.s3_load_data_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_load_write_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.s3_load_write_policy.arn
}

# --------------------------------------
# Extract Lambda IAM Policy for CloudWatch
# --------------------------------------

# Define
data "aws_iam_policy_document" "extract_cw_document" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extract_lambda}:*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "extract_cw_policy" {
  name_prefix = "cw-policy-${var.extract_lambda}"
  policy      = data.aws_iam_policy_document.extract_cw_document.json
}


# Attach
resource "aws_iam_role_policy_attachment" "extract_lambda_cw_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.extract_cw_policy.arn
}


# ------------------------------------
# Extract Lambda IAM Policy for Secrets Manager
# ------------------------------------

# Define
data "aws_iam_policy_document" "secrets_manager_policy_doc" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
    effect    = "Allow"
  }
}

# Create
resource "aws_iam_policy" "secrets_manager_policy" {
  name_prefix = "secrets-manager-policy-${var.extract_lambda}-"
  policy      = data.aws_iam_policy_document.secrets_manager_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "secrets_manager_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.secrets_manager_policy.arn
}


# --------------------------------------
# Load Lambda IAM Policy for CloudWatch
# --------------------------------------

# Define
data "aws_iam_policy_document" "extract_cw_document" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extract_lambda}:*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "extract_cw_policy" {
  name_prefix = "cw-policy-${var.extract_lambda}"
  policy      = data.aws_iam_policy_document.extract_cw_document.json
}


# Attach
resource "aws_iam_role_policy_attachment" "extract_lambda_cw_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.extract_cw_policy.arn
}


# ------------------------------------
# Load Lambda IAM Policy for Secrets Manager
# ------------------------------------

# Define

# Create
resource "aws_iam_policy" "secrets_manager_policy" {
  name_prefix = "secrets-manager-policy-${var.load_lambda}-"
  policy      = data.aws_iam_policy_document.secrets_manager_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "secrets_manager_policy_attachment" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.secrets_manager_policy.arn
}



# --------------------------------------
# Transform Lambda IAM Policy for S3 Write
# --------------------------------------

# Define

data "aws_iam_policy_document" "s3_transform_data_policy_doc" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:GetObject"
    ]
    resources = [
      "${aws_s3_bucket.ingested_data_bucket.arn}",
      "${aws_s3_bucket.ingested_data_bucket.arn}/*"
    ]
    effect = "Allow"
  }
  statement {
    actions = [
      "s3:PutObject",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.onyx_processed_bucket.arn}",
      "${aws_s3_bucket.onyx_processed_bucket.arn}/*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "s3_transform_write_policy" {
  name_prefix = "s3-policy-${var.transform_lambda}-write"
  policy      = data.aws_iam_policy_document.s3_transform_data_policy_doc.json
}


# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_transform_write_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.s3_transform_write_policy.arn
}


# --------------------------------------
# Transform Lambda IAM Policy for CloudWatch
# --------------------------------------

# Define
data "aws_iam_policy_document" "transform_cw_document" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.transform_lambda}:*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "transform_cw_policy" {
  name_prefix = "cw-policy-${var.transform_lambda}"
  policy      = data.aws_iam_policy_document.transform_cw_document.json
}


# Attach
resource "aws_iam_role_policy_attachment" "transform_lambda_cw_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_cw_policy.arn
}
