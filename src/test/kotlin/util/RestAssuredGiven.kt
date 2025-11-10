package util

import io.restassured.RestAssured
import io.restassured.filter.log.RequestLoggingFilter
import io.restassured.filter.log.ResponseLoggingFilter
import io.restassured.http.ContentType


/**
 *  Given em comum
 */
fun givenSearch() =
    RestAssured.given().relaxedHTTPSValidation()
        .filter(RequestLoggingFilter())
        .filter(ResponseLoggingFilter())
        .contentType(ContentType.JSON)
        .accept( "*/*")
        .header("Accept-Language", "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6")
        .header("Connection", "keep-alive")
        .header("Cookie", "JSESSIONID=AB8C3A2327FD919ABD6FD22BB33A23E7.ui-1; ys-rsys.lists.details.stats.tabs=o%3AactiveTab%3Dn%253A0; ys-rsys.lists.grids.filters.grid=o%3Acolumns%3Da%253Ao%25253Aid%25253Dn%2525253A0%25255Ewidth%25253Dn%2525253A308%255Eo%25253Aid%25253Dn%2525253A1%25255Ewidth%25253Dn%2525253A249%255Eo%25253Aid%25253Dn%2525253A2%25255Ewidth%25253Dn%2525253A193%255Eo%25253Aid%25253Dn%2525253A3%25255Ewidth%25253Dn%2525253A193%255Eo%25253Aid%25253Dn%2525253A4%25255Ewidth%25253Dn%2525253A193%255Eo%25253Aid%25253Dn%2525253A5%25255Ewidth%25253Dn%2525253A193%255Eo%25253Aid%25253Dn%2525253A6%25255Ewidth%25253Dn%2525253A193%255Eo%25253Aid%25253Dn%2525253A7%25255Ewidth%25253Dn%2525253A193%25255Ehidden%25253Db%2525253A1%5Esort%3Do%253Afield%253Ds%25253Aname%255Edirection%253Ds%25253AASC; ys-rsys.lists.selector=o%3Avalue%3Ds%253APagSeguro; ys-rsys.lists.details=o%3Acollapsed%3Db%253A0; ys-overWini18n_menu_filter_move=o%3Awidth%3Dn%253A823%5Eheight%3Dn%253A545%5Ex%3Dn%253A151%5Ey%3Dn%253A122; ys-rsys.lists.grids.profileExtensions.grid=o%3Acolumns%3Da%253Ao%25253Aid%25253Dn%2525253A0%25255Ewidth%25253Dn%2525253A329%255Eo%25253Aid%25253Dn%2525253A1%25255Ewidth%25253Dn%2525253A267%255Eo%25253Aid%25253Dn%2525253A2%25255Ewidth%25253Dn%2525253A203%255Eo%25253Aid%25253Dn%2525253A3%25255Ewidth%25253Dn%2525253A203%25255Ehidden%25253Db%2525253A1%255Eo%25253Aid%25253Dn%2525253A4%25255Ewidth%25253Dn%2525253A203%255Eo%25253Aid%25253Dn%2525253A5%25255Ewidth%25253Dn%2525253A203%255Eo%25253Aid%25253Dn%2525253A6%25255Ewidth%25253Dn%2525253A203%5Esort%3Do%253Afield%253Ds%25253Aname%255Edirection%253Ds%25253ADESC; ys-overWini18n_menu_extension_viewhistory=o%3Awidth%3Dn%253A823%5Eheight%3Dn%253A545%5Ex%3Dn%253A324%5Ey%3Dn%253A132; ys-overWinviewRecords=o%3Awidth%3Dn%253A1140%5Eheight%3Dn%253A617%5Ex%3Dn%253A16%5Ey%3Dn%253A70; ys-overWini18n_menu_extension_change_properties=o%3Awidth%3Dn%253A823%5Eheight%3Dn%253A545%5Ex%3Dn%253A217%5Ey%3Dn%253A167; ys-overWini18n_menu_extension_viewdata=o%3Awidth%3Dn%253A1223%5Eheight%3Dn%253A621%5Ex%3Dn%253A33%5Ey%3Dn%253A109; ys-rsys.lists.grids.profileExtensions.profile=o%3AselectedFolderId%3Dn%253A4402%5EselectedCreatorId%3Ds%253ASelect%2520All; ys-rsys.lists.tabs=o%3AactiveTab%3Dn%253A0; ys-overWini18n_menu_filter_viewdata=o%3Awidth%3Dn%253A823%5Eheight%3Dn%253A545%5Ex%3Dn%253A313%5Ey%3Dn%253A114; ys-overWinviewPushListRecord=o%3Awidth%3Dn%253A1531%5Eheight%3Dn%253A694%5Ex%3Dn%253A17%5Ey%3Dn%253A85; ys-rsys.lists.grids.filters.filter=o%3AselectedFolderId%3Dn%253A2254502%5EselectedCreatorId%3Ds%253ASelect%2520All; RI_CSRF=24ad86ce-5764-4475-a308-c42342416dc1; ORA_FPC=id=c081b4db-6cff-4385-8cbc-b3fb34875368:lv=1701185735182:ss=1701185735182; RI=v-627_ECuevedVZwYfyKvl8A4P7RMD3uEha5jsh3_v2rW2J0kPBuEo5mq--tIn0rxO-_lAl4xFPDQLLgHqGzcDqM2TjZLmIHV-bh4RVlzmxQlkIFvMFRZH-7ADFcb8m0yD1VIZD8basClRBs8yLXNBYixu2ztkL38QPDXsdRAzIjHN-lBVzjstJiKCwq4ARoQo73BuvbJ-1gH9zFhgWV7UTl2WJyNsKXFK8C-keM1-X4p2F5XnojEfftmRMMCFOokJ5GCr1tX-cKHpZqFaBd-J1tYl; JSESSIONID=949B4CAA57FAF970B1CE47D4E3467F70.ui-0; RI=v-627_EE1qeP_vy5NmFYJ0LwOXYpPRq-U49pIOT_iTxDUFKDkxBSAKTPrG1A7Y3Juk9leRQRnQ35A3g6rWOA17zzHZzUFUa4ST8RD42-8Bg78RNjUnlpeYC9fyujDguzgZ39SgIXD5zSNmj-zJvjYyX3X1wkJVCsEm0o69pvlGSS7QhKraLiMffJqIu4JrbKRsn1EcUDPz0T-_wUf_nzz0EYQvlRQHDNnZiWWayxwtMt7156VUCUi0uKCrdG_MeHmqp-8ljtitFX1UebWjhvyDGjra")
        .header("Referer", "https://pk21qan.responsys.ocs.oraclecloud.com/interact/jsp/en/testfilters.jsp?id=3362462&type=User_Filter")
        .header("Sec-Fetch-Dest", "empty")
        .header("Sec-Fetch-Mode", "cors")
        .header("Sec-Fetch-Site", "same-origin")
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
        .header("sec-ch-ua", "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"")
        .header("sec-ch-ua-mobile", "?0")
        .header("sec-ch-ua-platform", "\"macOS\"")

fun givenCreateAcceptAndJson() =
    RestAssured.given().relaxedHTTPSValidation()
        .filter(RequestLoggingFilter())
        .filter(ResponseLoggingFilter())
        .contentType(ContentType.JSON)
        .accept( ContentType.JSON)

fun givenCreateAccept() =
    RestAssured.given().relaxedHTTPSValidation()
        .filter(RequestLoggingFilter())
        .filter(ResponseLoggingFilter())
        .accept( "*/*")