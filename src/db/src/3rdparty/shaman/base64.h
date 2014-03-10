
/** @file util/crypt/base64.h
  * @brief Base64 encoding
  * @author Apache Software Foundation
  * $Revision: 1.1 $
  */

#ifndef __BASE64_H__
#define __BASE64_H__

/* base64 functions */
extern int ap_base64decode_len(const char *bufcoded, int buflen);
extern int ap_base64decode(char *bufplain, const char *bufcoded, int buflen);
extern int ap_base64decode_binary(unsigned char *bufplain, const char *bufcoded, int buflen);
extern int ap_base64encode_len(int len);
extern int ap_base64encode(char *encoded, const char *string, int len);
extern int ap_base64encode_binary(char *encoded, const unsigned char *string, int len);

/* convenience, result string must be free()'d by caller */
extern char *b64_encode(char *buf, int len);
extern char *b64_decode(char *buf);

#endif
