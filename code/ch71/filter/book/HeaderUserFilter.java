// Chapter 71, Step 4: a toy authentication filter for the Spark UI.
// It trusts the X-Book-User header and makes it the remote user, which the UI ACLs then check.
// A real deployment puts a filter here that validates a token (Kerberos/SPNEGO, OIDC, a proxy's
// signed header). Never trust a plain header from the network as this toy does.
package book;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;

public class HeaderUserFilter implements Filter {
  private String header = "X-Book-User";

  @Override
  public void init(FilterConfig config) {
    String h = config.getInitParameter("header");   // spark.book.HeaderUserFilter.param.header
    if (h != null) header = h;
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest http = (HttpServletRequest) req;
    final String user = http.getHeader(header);
    chain.doFilter(new HttpServletRequestWrapper(http) {
      @Override
      public String getRemoteUser() {
        return user;
      }
    }, res);
  }
}
