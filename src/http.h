#ifndef MERLIN_HTTP_H
#define MERLIN_HTTP_H

void httpServerInit();
void httpHandler(uWS::HttpResponse *res, uWS::HttpRequest req, char *data, size_t length, size_t remainingBytes);

#endif //MERLIN_HTTP_H
