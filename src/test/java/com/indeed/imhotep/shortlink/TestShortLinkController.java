package com.indeed.imhotep.shortlink;

import com.google.common.collect.ImmutableMap;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.servlet.view.RedirectView;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class TestShortLinkController {

    private HttpServletRequest request;
    private HttpServletResponse response;
    private ShortLinkRepository mockRepo;
    private ShortLinkController controller;

    @Before
    public void init() {
        request = createMock(HttpServletRequest.class);
        response = createMock(HttpServletResponse.class);
        mockRepo = createMock(ShortLinkRepository.class);
    }

    private void assertCreateResponse(String status, String urlPattern, Object response) {
        ImmutableMap<String, String> map = (ImmutableMap<String, String>) response;
        assertEquals("response status", status, map.get("status"));
        if (urlPattern != null) {
            assertTrue("short code URL", map.get("url").matches(urlPattern));
        } else {
            assertFalse("no URL returned", map.containsKey("url"));
        }

    }

    @Test
    public void testCreate() throws Exception {

        expect(mockRepo.isEnabled()).andReturn(true).anyTimes();

        response.setHeader((String) anyObject(), (String) anyObject());
        expectLastCall().times(4);

        final String params = "q[]=from%20mydataset%202016-01-01%202016-01-02";
        expect(mockRepo.mapShortCode(matches("[A-Z0-9]{6}"), eq(params))).andReturn(true).once();

        replay(mockRepo, request, response);

        final UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl("http://imhotep");
        controller = new ShortLinkController(mockRepo);
        assertCreateResponse("ok", "http://imhotep/q/[A-Z0-9]{6}",
                controller.create(request, response, uriBuilder, params));

        verify(mockRepo, request, response);

    }

    @Test
    public void testCreate_retriesSucceed() throws Exception {

        expect(mockRepo.isEnabled()).andReturn(true).anyTimes();

        response.setHeader((String) anyObject(), (String) anyObject());
        expectLastCall().times(4);

        final String params = "q[]=from%20mydataset%202016-01-01%202016-01-02";
        expect(mockRepo.mapShortCode(matches("[A-Z0-9]{6}"), eq(params))).andReturn(false).times(49);
        expect(mockRepo.mapShortCode(matches("[A-Z0-9]{6}"), eq(params))).andReturn(true).once();

        replay(mockRepo, request, response);

        final UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl("http://imhotep");
        controller = new ShortLinkController(mockRepo);
        assertCreateResponse("ok", "http://imhotep/q/[A-Z0-9]{6}",
                controller.create(request, response, uriBuilder, params));

        verify(mockRepo, request, response);

    }

    @Test
    public void testCreate_retriesFail() throws Exception {

        expect(mockRepo.isEnabled()).andReturn(true).anyTimes();

        response.setHeader((String) anyObject(), (String) anyObject());
        expectLastCall().times(4);

        final String params = "q[]=from%20mydataset%202016-01-01%202016-01-02";
        expect(mockRepo.mapShortCode(matches("[A-Z0-9]{6}"), eq(params))).andReturn(false).times(50);

        replay(mockRepo, request, response);

        final UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl("http://imhotep");
        controller = new ShortLinkController(mockRepo);
        try {
            controller.create(request, response, uriBuilder, params);
            fail("Should have thrown exception");
        } catch (RuntimeException e) {
            // expected
        }

        verify(mockRepo, request, response);

    }

    @Test
    public void testCreate_disabled() throws Exception {

        expect(mockRepo.isEnabled()).andReturn(false).anyTimes();

        replay(mockRepo, request, response);

        final UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl("http://imhotep");
        controller = new ShortLinkController(mockRepo);
        assertCreateResponse("disabled", null,
                controller.create(request, response, uriBuilder, ""));

        verify(mockRepo, request, response);

    }

    @Test
    public void testRedirect() throws Exception {
        expect(mockRepo.isEnabled()).andReturn(true).anyTimes();

        final String params = "q[]=from%20mydataset%202016-01-01%202016-01-02";
        expect(mockRepo.resolveShortCode(matches("[A-Z0-9]{6}"))).andReturn(params).once();

        replay(mockRepo, request, response);

        controller = new ShortLinkController(mockRepo);
        RedirectView view = (RedirectView) controller.redirect("JFA39D");
        assertEquals("/iql/#" + params, view.getUrl());

        verify(mockRepo, request, response);

    }

    @Test
    public void testRedirect_disabled() throws Exception {
        expect(mockRepo.isEnabled()).andReturn(false).anyTimes();

        replay(mockRepo, request, response);

        controller = new ShortLinkController(mockRepo);
        RedirectView view = (RedirectView) controller.redirect("JFA39D");
        assertEquals("/iql/", view.getUrl());

        verify(mockRepo, request, response);

    }
}
