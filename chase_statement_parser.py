import pdfplumber
import csv
import re
import argparse
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChaseStatementProcessor:
    def __init__(self, debug=False):
        """Initialize the Chase statement processor."""
        if debug:
            logger.setLevel(logging.DEBUG)
        
        # Define category keywords for transaction classification
        self.category_keywords = {
            'FOOD_DINING': [
                'RESTAURANT', 'CAFE', 'COFFEE', 'DINER', 'BISTRO', 'GRILL', 'BURGER', 'PIZZA', 'SUSHI',
                'TACO', 'THAI', 'CHINESE', 'ITALIAN', 'MEXICAN', 'JAPANESE', 'BBQ', 'STEAKHOUSE',
                'DOORDASH', 'GRUBHUB', 'UBEREATS', 'SEAMLESS', 'POSTMATES', 'BAR', 'PUB', 'BREWERY',
                'MCDONALD', 'WENDY', 'CHIPOTLE', 'PANERA', 'STARBUCKS', 'DUNKIN', 'PHO', 'BANH MI',
                'BLUESTONE LANE'
            ],
            'GROCERIES': [
                'WHOLE FOODS', 'WHOLEFDS', 'SAFEWAY', 'GROCERY', 'MARKET', 'SUPERMARKET', 'TRADER JOE', 'KROGER',
                'ALBERTSONS', 'PUBLIX', 'ALDI', 'WEGMANS', 'COSTCO', 'FOOD LION', 'GIANT', 'MEIJER',
                'VONS', 'SHOPRITE', 'SPROUTS', 'FRESH MARKET', 'ORGANIC', 'FARMERS MARKET', 'SEES CANDY',
                'BI-RITE MARKET', 'RAINBOW GROCERY', 'DRAEGER\'S', 'TRADE COFFEE'
            ],
            'HOUSEHOLD_GOODS': [
                'TARGET', 'AMAZON', 'WALGREENS', 'CVS', 'HOME DEPOT', 'LOWE\'S', 'BED BATH',
                'IKEA', 'WAYFAIR', 'HARDWARE', 'CONTAINER STORE', 'CRATE BARREL', 'WILLIAM SONOMA',
                'POTTERY BARN', 'BATH BODY WORKS', 'WALMART', 'COSTCO', 'BJ\'S', 'SAM\'S CLUB',
                'UPS STORE', 'JACK\'S LAUNDRY', 'LAUNDRY'
            ],
            'WELLNESS': [
                'MASSAGE', 'SPA', 'NAIL', 'SALON', 'BEAUTY', 'FACIAL', 'WAXING', 'MANICURE',
                'PEDICURE', 'HAIR', 'BARBER', 'STYLIST', 'COSMETIC', 'ESTHETICIAN', 'SKIN CARE'
            ],
            'ACTIVITIES': [
                'DOJO', 'GYM', 'FITNESS', 'SPORT', 'RECREATION', 'YOGA', 'PILATES', 'CROSSFIT',
                'CYCLING', 'DANCE', 'CLIMBING', 'MARTIAL ARTS', 'POOL', 'TENNIS', 'GOLF',
                'BOWLING', 'MUSEUM', 'THEATER', 'CINEMA', 'MOVIE', 'CONCERT', 'AQUARIUM', 'ZOO',
                'SHIVWORKS', 'SAN FRANCISCO REC & PARKS'
            ],
            'SHOPPING': [
                'CLOTHING', 'RETAIL', 'DEPARTMENT', 'STORE', 'BOUTIQUE', 'MACY', 'NORDSTROM', 
                'SHOES', 'APPAREL', 'FASHION', 'OUTLET', 'MALL', 'FOOTWEAR', 'ACCESSORY', 'JEWELRY',
                'WATCH', 'HANDBAG', 'NIKE', 'ADIDAS', 'GAP', 'ZARA', 'H&M', 'OLD NAVY', 'J CREW',
                'LLBEAN', 'LULULEMON', 'SEPHORA', 'NEIMANMARCUS', 'ALO-YOGA',
                'WILSON SPORTING', 'BODEN', 'VUORI', '20 SPOT'
            ],
            'TRANSPORT': [
                'UBER', 'LYFT', 'TAXI', 'TRANSIT', 'SUBWAY', 'BUS', 'TRAIN', 'TRANSPORT',
                'METRO', 'RAIL', 'COMMUTER', 'FERRY', 'TROLLEY', 'PARKING', 'GARAGE', 'TOLL',
                'GAS', 'FUEL', 'CHARGING', 'CAR WASH', 'AUTO', 'VEHICLE', 'STATE FARM', 
                'CLIPPER', 'SFMTA', 'PARKMOBILE', 'CALTRAIN'
            ],
            'SUBSCRIPTIONS': [
                'SPOTIFY', 'SUBSCRIPTION', 'MONTHLY', 'DIGITAL', 'SERVICE',
                'APPLE', 'GOOGLE', 'AMAZON PRIME', 'DISNEY+', 'HBO', 'AUDIBLE', 'XBOX', 'PLAYSTATION',
                'NINTENDO', 'NEWSPAPER', 'MAGAZINE', 'JOURNAL', 'APP', 'SOFTWARE', 'CLOUD',
                'CHATGPT', 'EXPRESSVPN', 'NYTIMES', 'BUSINESS INSIDER'
            ],
            'HEALTH': [
                'MEDICAL', 'PHARMACY', 'CLINIC', 'DOCTOR', 'HOSPITAL', 'DENTIST', 'HEALTH',
                'PHYSICIAN', 'OPTOMETRIST', 'OPTICAL', 'GLASSES', 'CONTACTS', 'THERAPY', 'COUNSELING',
                'LABORATORY', 'TEST', 'IMAGING', 'SPECIALIST', 'EMERGENCY', 'AMBULANCE', 'PRESCRIPTION',
                'ONEMED'
            ],
            'UTILITIES': [
                'BILL', 'INSURANCE', 'PHONE', 'INTERNET', 'UTILITY', 'ELECTRIC', 'GAS', 'WATER',
                'SEWER', 'TRASH', 'WIRELESS', 'CABLE', 'SATELLITE', 'TV', 'STREAMING', 'CELL',
                'MOBILE', 'DATA', 'HOME', 'AUTO', 'LIFE', 'HEALTH', 'DENTAL', 'VISION', 'ATT',
                'AT&T', 'ATT*BILL'
            ],
            'TRAVEL': [
                'AIRLINE', 'HOTEL', 'MOTEL', 'VACATION', 'RENTAL', 'TRAVEL', 'FLIGHT', 'AIRBNB',
                'VRBO', 'BOOKING', 'EXPEDIA', 'TRIP', 'CRUISE', 'RESORT', 'LODGE', 'INN',
                'AIRPORT', 'TSA', 'BAGGAGE', 'TAXI', 'SHUTTLE', 'PARKING', 'CAR RENTAL'
            ],
            'ENTERTAINMENT': [
                'NETFLIX', 'HULU', 'MOVIES', 'SHOW', 'THEATER', 'CINEMA', 'STREAMING',
                'HBO', 'DISNEY+', 'PARAMOUNT', 'SHOWTIME', 'ENTERTAINMENT', 'FILM', 'TV'
            ]
        }

    def parse_pdf(self, pdf_path):
        """
        Parse Chase credit card statement PDF and extract transactions.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            List of transaction dictionaries
        """
        transactions = []
        current_section = None
        date_pattern = r'\d{2}/\d{2}'  # MM/DD format
        
        logger.info(f"Processing PDF: {pdf_path}")
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    logger.info(f"Processing page {page_num} of {len(pdf.pages)}")
                    text = page.extract_text()
                    lines = text.split('\n')
                    
                    for line_num, line in enumerate(lines, 1):
                        # Check for section headers
                        if re.search(r'PURCHASES', line, re.IGNORECASE):
                            current_section = 'PURCHASES'
                            logger.debug(f"Entered section: {current_section}")
                            continue
                        elif re.search(r'PAYMENTS AND OTHER CREDITS', line, re.IGNORECASE):
                            current_section = 'PAYMENTS_AND_CREDITS'
                            logger.debug(f"Entered section: {current_section}")
                            continue
                        elif re.search(r'FEES CHARGED', line, re.IGNORECASE):
                            current_section = 'FEES'
                            logger.debug(f"Entered section: {current_section}")
                            continue
                        elif re.search(r'INTEREST CHARGED', line, re.IGNORECASE):
                            current_section = 'INTEREST'
                            logger.debug(f"Entered section: {current_section}")
                            continue
                        elif re.search(r'ADJUSTMENTS', line, re.IGNORECASE):
                            current_section = 'ADJUSTMENTS'
                            logger.debug(f"Entered section: {current_section}")
                            continue
                        
                        # Skip lines that don't look like transactions
                        if not re.search(date_pattern, line):
                            continue
                        
                        # Process transaction based on section
                        transaction = None
                        if current_section == 'PURCHASES':
                            transaction = self._parse_purchase(line)
                            if transaction:
                                # Force transaction type to be Purchase
                                transaction['type'] = 'Purchase'
                        elif current_section == 'PAYMENTS_AND_CREDITS':
                            transaction = self._parse_payment_or_credit(line)
                        elif current_section == 'FEES':
                            transaction = self._parse_fee(line)
                        elif current_section == 'INTEREST':
                            transaction = self._parse_fee(line)
                            if transaction:
                                transaction['type'] = 'Interest'
                        elif current_section == 'ADJUSTMENTS':
                            transaction = self._parse_adjustment(line)
                        
                        if transaction:
                            transactions.append(transaction)
                            logger.debug(f"Found transaction: {transaction}")
        except Exception as e:
            logger.error(f"Error processing PDF: {str(e)}")
            raise
        
        logger.info(f"Extracted {len(transactions)} transactions from PDF")
        return transactions

    def _parse_transaction_line(self, line):
        """
        Parse a transaction line to extract date, description, and amount.
        
        Args:
            line: The line of text from the PDF
            
        Returns:
            Tuple of (date, description, amount) or None if parsing fails
        """
        # Try different patterns to handle variations in formatting
        
        # Standard format: MM/DD DESCRIPTION $AMOUNT
        pattern1 = '(\\d{2}/\\d{2})\\s+(.*?)\\s+(\\$[\\d,]+\\.\\d{2})$'
        
        # Format with trailing spaces: MM/DD DESCRIPTION $AMOUNT 
        pattern2 = '(\\d{2}/\\d{2})\\s+(.*?)\\s+(\\$[\\d,]+\\.\\d{2})\\s+'
        
        # Format without $ sign: MM/DD DESCRIPTION AMOUNT
        pattern3 = '(\\d{2}/\\d{2})\\s+(.*?)\\s+(\\d{1,3}(?:,\\d{3})*\\.\\d{2})$'
        
        # Format for negative amounts: MM/DD DESCRIPTION -AMOUNT
        pattern4 = '(\\d{2}/\\d{2})\\s+(.*?)\\s+-(\\d{1,3}(?:,\\d{3})*\\.\\d{2})$'
        
        # Format for negative amounts with $ sign: MM/DD DESCRIPTION -$AMOUNT
        pattern5 = '(\\d{2}/\\d{2})\\s+(.*?)\\s+-\\$(\\d{1,3}(?:,\\d{3})*\\.\\d{2})$'
        
        patterns = [pattern1, pattern2, pattern3, pattern4, pattern5]
        
        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                date, description, amount_str = match.groups()
                
                # Clean up the amount string and convert to float
                amount_str = amount_str.replace('$', '').replace(',', '')
                try:
                    amount = float(amount_str)
                    
                    # For patterns 4 and 5 (negative amounts), negate the amount
                    if pattern in [pattern4, pattern5]:
                        amount = -amount
                        
                    return date, description.strip(), amount
                except ValueError:
                    logger.warning(f"Failed to convert amount to float: {amount_str}")
                    continue
        
        logger.warning(f"Failed to parse transaction line: {line}")
        return None

    def _parse_purchase(self, line):
        """Parse a line from the Purchases section."""
        parsed = self._parse_transaction_line(line)
        if parsed:
            date, description, amount = parsed
            return {
                'date': date,
                'description': description,
                'amount': amount,
                'type': 'Purchase',
                'category': self.categorize_transaction(description)
            }
        return None

    def _parse_payment_or_credit(self, line):
        """Parse a line from the Payments and Credits section."""
        parsed = self._parse_transaction_line(line)
        if parsed:
            date, description, amount = parsed
            
            # Determine if it's a payment, return, or regular purchase
            # Payments typically contain specific keywords
            is_payment = any(keyword in description.upper() for keyword in ['PAYMENT', 'THANK YOU', 'AUTOPAY'])
            
            # Determine transaction type
            if is_payment:
                transaction_type = 'Payment'
                category = 'Payment'
                # Ensure amount is negative for payments (reducing balance)
                if amount > 0:
                    amount = -amount
            elif amount < 0:
                # Negative amount indicates a return/credit
                transaction_type = 'Return'
                category = self.categorize_transaction(description)
            else:
                # If it's not a payment and not negative, it's likely a purchase
                transaction_type = 'Purchase'
                category = self.categorize_transaction(description)
            
            logger.debug(f"Payment/Credit: '{description}', identified as: {transaction_type}")
            
            return {
                'date': date,
                'description': description,
                'amount': amount,
                'type': transaction_type,
                'category': category
            }
        return None

    def _parse_fee(self, line):
        """Parse a line from the Fees section."""
        parsed = self._parse_transaction_line(line)
        if parsed:
            date, description, amount = parsed
            return {
                'date': date,
                'description': description,
                'amount': amount,
                'type': 'Fee',
                'category': 'Fee'
            }
        return None

    def _parse_adjustment(self, line):
        """Parse a line from the Adjustments section."""
        parsed = self._parse_transaction_line(line)
        if parsed:
            date, description, amount = parsed
            return {
                'date': date,
                'description': description,
                'amount': amount,
                'type': 'Adjustment',
                'category': 'Adjustment'
            }
        return None

    def categorize_transaction(self, description):
        """
        Categorize a transaction based on its description.
        
        Args:
            description: The transaction description
            
        Returns:
            Category string
        """
        # Convert to uppercase for case-insensitive matching
        desc_upper = description.upper()
        
        # Check each category for matching keywords
        for category, keywords in self.category_keywords.items():
            if any(keyword in desc_upper for keyword in keywords):
                return category
        
        # Default category if no match is found
        return 'UNCATEGORIZED'

    def export_to_csv(self, transactions, output_path):
        """
        Write transactions to a CSV file in Chase's format.
        
        Args:
            transactions: List of transaction dictionaries
            output_path: Path to write the CSV file
        """
        # Define the headers based on Chase's format
        headers = ['Transaction Date', 'Post Date', 'Description', 'Category', 'Type', 'Amount']
        
        logger.info(f"Writing {len(transactions)} transactions to {output_path}")
        try:
            with open(output_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                
                for transaction in transactions:
                    # Convert our transaction format to Chase's format
                    writer.writerow({
                        'Transaction Date': transaction['date'],
                        'Post Date': transaction['date'],  # We might not have post date info
                        'Description': transaction['description'],
                        'Category': transaction['category'],
                        'Type': transaction['type'],
                        'Amount': f"${abs(transaction['amount']):.2f}"
                    })
            logger.info(f"Successfully wrote transactions to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error writing to CSV: {str(e)}")
            return False

    def validate_against_chase_csv(self, our_csv_path, chase_csv_path):
        """
        Compare our generated CSV with Chase's downloaded CSV.
        
        Args:
            our_csv_path: Path to our generated CSV
            chase_csv_path: Path to Chase's downloaded CSV
            
        Returns:
            Dictionary with validation results
        """
        our_transactions = []
        chase_transactions = []
        
        logger.info(f"Validating our CSV ({our_csv_path}) against Chase's CSV ({chase_csv_path})")
        
        # Read our CSV
        try:
            with open(our_csv_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    our_transactions.append(row)
            logger.info(f"Read {len(our_transactions)} transactions from our CSV")
        except Exception as e:
            logger.error(f"Error reading our CSV: {str(e)}")
            raise
        
        # Read Chase's CSV
        try:
            with open(chase_csv_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    chase_transactions.append(row)
            logger.info(f"Read {len(chase_transactions)} transactions from Chase's CSV")
        except Exception as e:
            logger.error(f"Error reading Chase's CSV: {str(e)}")
            raise
        
        # Compare the transactions
        missing_in_ours = []
        missing_in_chase = []
        
        for chase_tx in chase_transactions:
            found = False
            for our_tx in our_transactions:
                # Compare the key fields (some columns might have slightly different names)
                chase_date = chase_tx.get('Transaction Date', '')
                chase_desc = chase_tx.get('Description', '')
                chase_amount = chase_tx.get('Amount', '')
                
                our_date = our_tx.get('Transaction Date', '')
                our_desc = our_tx.get('Description', '')
                our_amount = our_tx.get('Amount', '')
                
                if (our_date == chase_date and 
                    our_desc == chase_desc and 
                    our_amount == chase_amount):
                    found = True
                    break
            if not found:
                missing_in_ours.append(chase_tx)
        
        for our_tx in our_transactions:
            found = False
            for chase_tx in chase_transactions:
                chase_date = chase_tx.get('Transaction Date', '')
                chase_desc = chase_tx.get('Description', '')
                chase_amount = chase_tx.get('Amount', '')
                
                our_date = our_tx.get('Transaction Date', '')
                our_desc = our_tx.get('Description', '')
                our_amount = our_tx.get('Amount', '')
                
                if (our_date == chase_date and 
                    our_desc == chase_desc and 
                    our_amount == chase_amount):
                    found = True
                    break
            if not found:
                missing_in_chase.append(our_tx)
        
        results = {
            'missing_in_ours': missing_in_ours,
            'missing_in_chase': missing_in_chase,
            'is_valid': len(missing_in_ours) == 0 and len(missing_in_chase) == 0
        }
        
        logger.info(f"Validation results: {len(missing_in_ours)} missing in ours, {len(missing_in_chase)} missing in Chase's")
        return results


def process_statement(processor, pdf_path, output_path, validate_path=None):
    """Process a single statement and generate CSV output.
    
    Args:
        processor: ChaseStatementProcessor instance
        pdf_path: Path to the PDF file
        output_path: Path to write CSV output
        validate_path: Optional path to Chase CSV for validation
        
    Returns:
        Dictionary with processing results
    """
    results = {
        'success': False,
        'transactions': 0,
        'validation': None
    }
    
    try:
        # Parse the statement
        logger.info(f"Processing statement: {pdf_path}")
        transactions = processor.parse_pdf(pdf_path)
        logger.info(f"Found {len(transactions)} transactions")
        results['transactions'] = len(transactions)
        
        # Write to CSV
        logger.info(f"Writing to CSV: {output_path}")
        if processor.export_to_csv(transactions, output_path):
            results['success'] = True
        
        # Validate if requested
        if validate_path:
            logger.info(f"Validating against: {validate_path}")
            validation_results = processor.validate_against_chase_csv(output_path, validate_path)
            results['validation'] = validation_results
            
            if validation_results['is_valid']:
                logger.info("Validation successful! All transactions match.")
            else:
                if validation_results['missing_in_ours']:
                    logger.warning(f"Found {len(validation_results['missing_in_ours'])} transactions in Chase's CSV that are missing in ours")
                    for tx in validation_results['missing_in_ours'][:5]:  # Show first 5 only
                        logger.warning(f"Missing: {tx}")
                    
                if validation_results['missing_in_chase']:
                    logger.warning(f"Found {len(validation_results['missing_in_chase'])} transactions in our CSV that are missing in Chase's")
                    for tx in validation_results['missing_in_chase'][:5]:  # Show first 5 only
                        logger.warning(f"Extra: {tx}")
    
    except Exception as e:
        logger.error(f"Error processing statement {pdf_path}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    return results

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Process Chase credit card statements')
    parser.add_argument('--statements_dir', '-d', help='Directory containing Chase statement PDFs', 
                        default='/Users/adriahou/Desktop/CreditCardAnalysis/credit_card_statements')
    parser.add_argument('--output_dir', '-o', help='Directory for output CSV files', 
                        default='.')
    parser.add_argument('--validate', '-v', help='Chase CSV file to validate against')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--single', '-s', help='Process a single PDF file instead of a directory')
    
    args = parser.parse_args()
    
    # Create processor
    processor = ChaseStatementProcessor(debug=args.debug)
    
    # Track overall results
    results = {
        'total_statements': 0,
        'successful_statements': 0,
        'total_transactions': 0
    }
    
    try:
        # Process a single file if specified
        if args.single:
            pdf_path = args.single
            if not os.path.exists(pdf_path):
                logger.error(f"File not found: {pdf_path}")
                return 1
                
            # Generate output filename
            filename = os.path.basename(pdf_path)
            base_name = os.path.splitext(filename)[0]
            output_path = os.path.join(args.output_dir, f"{base_name}.csv")
            
            # Process the PDF
            result = process_statement(processor, pdf_path, output_path, args.validate)
            
            # Update results
            results['total_statements'] = 1
            if result['success']:
                results['successful_statements'] = 1
                results['total_transactions'] = result['transactions']
        
        # Process all PDFs in the directory
        else:
            statements_dir = args.statements_dir
            if not os.path.exists(statements_dir):
                logger.error(f"Directory not found: {statements_dir}")
                return 1
            
            # Create output directory if it doesn't exist
            os.makedirs(args.output_dir, exist_ok=True)
            
            # Find all PDFs in the directory
            pdf_files = [f for f in os.listdir(statements_dir) if f.lower().endswith('.pdf')]
            
            if not pdf_files:
                logger.warning(f"No PDF files found in {statements_dir}")
                return 0
            
            logger.info(f"Found {len(pdf_files)} PDF files to process")
            results['total_statements'] = len(pdf_files)
            
            # Process each PDF
            for pdf_file in pdf_files:
                pdf_path = os.path.join(statements_dir, pdf_file)
                
                # Generate output filename
                base_name = os.path.splitext(pdf_file)[0]
                output_path = os.path.join(args.output_dir, f"{base_name}.csv")
                
                # Process the PDF
                result = process_statement(processor, pdf_path, output_path, args.validate)
                
                # Update results
                if result['success']:
                    results['successful_statements'] += 1
                    results['total_transactions'] += result['transactions']
        
        # Print overall results
        logger.info(f"Processing completed. Summary:")
        logger.info(f"  Statements processed: {results['successful_statements']}/{results['total_statements']}")
        logger.info(f"  Total transactions extracted: {results['total_transactions']}")
        
        return 0
    
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == '__main__':
    exit(main())
