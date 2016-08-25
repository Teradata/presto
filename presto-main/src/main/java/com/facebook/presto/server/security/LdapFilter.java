/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server.security;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.net.HttpHeaders;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.naming.AuthenticationException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.Principal;
import java.util.Base64;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.naming.Context.INITIAL_CONTEXT_FACTORY;
import static javax.naming.Context.PROVIDER_URL;
import static javax.naming.Context.SECURITY_AUTHENTICATION;
import static javax.naming.Context.SECURITY_CREDENTIALS;
import static javax.naming.Context.SECURITY_PRINCIPAL;

public class LdapFilter
        implements Filter
{
    private static final Logger LOG = Logger.get(LdapFilter.class);
    public static final String AUTHENTICATION_TYPE = "basic";
    public static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
    private final String ldapUrl;
    private final LdapBinder ldapBinder;
    private final Optional<String> groupDistinguishedName;
    private final Optional<String> baseDistinguishedName;
    private final Optional<String> userObjectClass;

    @Inject
    public LdapFilter(LdapServerConfig config, LdapBinder ldapBinder)
    {
        this.ldapUrl = requireNonNull(config.getLdapUrl(), "ldapUrl is null");
        this.ldapBinder = requireNonNull(ldapBinder, "ldapBinder is null");
        this.groupDistinguishedName = Optional.ofNullable(config.getGroupDistinguishedName());
        this.baseDistinguishedName = Optional.ofNullable(config.getBaseDistinguishedName());
        this.userObjectClass = Optional.ofNullable(config.getUserObjectClass());

        try {
            authenticate(getBasicEnvironment());
        }
        catch (NamingException e) {
            throw Throwables.propagate(e);
        }
    }

    private InitialDirContext authenticate(Hashtable<String, String> environment)
            throws NamingException
    {
        return new InitialDirContext(environment);
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        // skip auth for http
        if (!servletRequest.isSecure()) {
            LOG.debug("Skipping LDAP authentication in HTTP.");
            nextFilter.doFilter(servletRequest, servletResponse);
            return;
        }

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);

        if (header != null) {
            List<String> parts = Splitter.onPattern("\\s+").splitToList(header);

            checkState(parts.size() == 2, "Invalid value for authorization header");
            checkState(parts.get(0).equalsIgnoreCase(AUTHENTICATION_TYPE), "Incorrect authentication type (expected basic): %s", AUTHENTICATION_TYPE);

            String[] credentials = new String(Base64.getDecoder().decode(parts.get(1))).split(":", 2);
            checkState(credentials.length == 2, "Username/password missing in the request header");

            DirContext context = null;
            try {
                String user = credentials[0];
                String password = credentials[1];
                if (user.isEmpty() || password.isEmpty()) {
                    throw new AuthenticationException("Authentication failed. Username or Password is empty");
                }

                Hashtable<String, String> environment = getBasicEnvironment();
                String principal = ldapBinder.getBindDistinguishedName(user);

                environment.put(SECURITY_AUTHENTICATION, "simple");
                environment.put(SECURITY_PRINCIPAL, principal);
                environment.put(SECURITY_CREDENTIALS, password);
                context = authenticate(environment);

                if (groupDistinguishedName.isPresent()) {
                    checkForGroupMembership(user, groupDistinguishedName.get(), context);
                }

                // ldap authentication ok, continue
                nextFilter.doFilter(new HttpServletRequestWrapper(request)
                {
                    @Override
                    public Principal getUserPrincipal()
                    {
                        return new LdapPrincipal(user);
                    }
                }, servletResponse);
                return;
            }
            catch (AuthenticationException e) {
                // Log exception since the error message from LDAP server is not very friendly to be returned to the user
                LOG.debug("LDAP Authentication failed with exception: %s", e);
                throw new RuntimeException("Authentication failed. Invalid credentials.");
            }
            catch (NamingException e) {
                throw Throwables.propagate(e);
            }
            finally {
                try {
                    if (context != null) {
                        context.close();
                    }
                }
                catch (NamingException e) {
                    // ignore
                }
            }
        }

        // request for user/password for LDAP authentication
        sendChallenge(response);
    }

    private Hashtable<String, String> getBasicEnvironment()
    {
        Hashtable<String, String> environment = new Hashtable<>();
        environment.put(INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY);
        environment.put(PROVIDER_URL, ldapUrl);
        return environment;
    }

    private static void sendChallenge(HttpServletResponse response)
    {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, format("%s realm=\"presto\"", AUTHENTICATION_TYPE));
    }

    @Override
    public void init(FilterConfig filterConfig)
            throws ServletException
    {
    }

    @Override
    public void destroy()
    {
    }

    private void checkForGroupMembership(String user, String groupDistinguishedName, DirContext context)
    {
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        String searchBase = baseDistinguishedName.get();
        try {
            String searchFilter = format("(&(objectClass=%s)(%s=%s)(memberof=%s))", userObjectClass.get(), ldapBinder.getUserSearchInput(), user, groupDistinguishedName);
            LOG.debug("Group membership check for user '%s' using query: %s and base distinguished name: %s", user, searchFilter, searchBase);
            NamingEnumeration<SearchResult> results = context.search(searchBase, searchFilter, searchControls);
            if (!results.hasMoreElements()) {
                throw new AuthenticationException(format("Authentication failed: User %s not a member of the group %s", user, groupDistinguishedName));
            }
        }
        catch (NamingException e) {
            throw Throwables.propagate(e);
        }
    }

    private static class LdapPrincipal
            implements Principal
    {
        private final String name;

        public LdapPrincipal(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LdapPrincipal that = (LdapPrincipal) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
